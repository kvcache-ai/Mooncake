#!/usr/bin/env python3
"""
ZMQ PUSH/PULL benchmark: Mooncake vs pyzmq.

Measures throughput, latency, and per-phase timing
(encode / send_api / recv_wait / decode / callback).

Usage:
  python3 zmq_timing_analysis.py --backend pyzmq --payload-kb 64 --duration 2
  PYTHONPATH=.../build/mooncake-integration python3 zmq_timing_analysis.py --backend both --payloads 4,64 --threads 2
"""

import argparse
import json
import os
import struct
import threading
import time
from dataclasses import asdict, dataclass
from statistics import mean

try:
    import zmq
except ImportError:
    zmq = None


def percentile(data, p):
    if not data:
        return 0.0
    s = sorted(data)
    idx = (len(s) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


def load_mooncake():
    for mod in ("mooncake.engine", "engine"):
        try:
            m = __import__(mod, fromlist=["ZmqConfig", "ZmqInterface", "ZmqSocketOption", "ZmqSocketType"])
            return m.ZmqConfig, m.ZmqInterface, m.ZmqSocketOption, m.ZmqSocketType
        except ImportError:
            continue
    raise ImportError("Cannot import mooncake zmq. Set PYTHONPATH to the mooncake-integration build dir.")


@dataclass
class Result:
    backend: str
    payload_kb: int
    threads: int
    duration_s: float
    sent_msgs: int
    recv_msgs: int
    msg_s: float
    mb_s: float
    e2e_p50_ms: float
    e2e_p95_ms: float
    encode_avg_us: float
    send_api_avg_us: float
    recv_wait_avg_us: float
    decode_avg_us: float
    callback_avg_us: float
    notes: str = ""


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.sent = self.recv = 0
        self.e2e_ms: list[float] = []
        self.encode_us: list[float] = []
        self.send_us: list[float] = []
        self.wait_us: list[float] = []
        self.decode_us: list[float] = []
        self.cb_us: list[float] = []

    def add_send(self, enc_us, snd_us):
        with self._lock:
            self.sent += 1
            self.encode_us.append(enc_us)
            self.send_us.append(snd_us)

    def add_recv(self, e2e_ms, wait_us, dec_us, cb_us):
        with self._lock:
            self.recv += 1
            self.e2e_ms.append(e2e_ms)
            self.wait_us.append(wait_us)
            self.decode_us.append(dec_us)
            self.cb_us.append(cb_us)

    def to_result(self, backend, payload_bytes, threads, elapsed, notes=""):
        n = self.recv
        avg = lambda lst: mean(lst) if lst else 0.0
        return Result(
            backend=backend,
            payload_kb=payload_bytes // 1024,
            threads=threads,
            duration_s=elapsed,
            sent_msgs=self.sent,
            recv_msgs=n,
            msg_s=n / max(elapsed, 1e-9),
            mb_s=n * payload_bytes / (1 << 20) / max(elapsed, 1e-9),
            e2e_p50_ms=percentile(self.e2e_ms, 0.50),
            e2e_p95_ms=percentile(self.e2e_ms, 0.95),
            encode_avg_us=avg(self.encode_us),
            send_api_avg_us=avg(self.send_us),
            recv_wait_avg_us=avg(self.wait_us),
            decode_avg_us=avg(self.decode_us),
            callback_avg_us=avg(self.cb_us),
            notes=notes,
        )


def run_pyzmq(payload_bytes, threads, duration_s, port, warmup_s):
    if zmq is None:
        raise RuntimeError("pyzmq not installed: pip install pyzmq")
    metrics = Metrics()
    stop = threading.Event()
    ready = threading.Event()
    body = b"x" * max(payload_bytes - 8, 0)
    addr = f"tcp://127.0.0.1:{port}"
    ctx = zmq.Context(io_threads=max(threads, 1))

    def receiver():
        sock = ctx.socket(zmq.PULL)
        sock.setsockopt(zmq.RCVHWM, 100_000)
        sock.bind(addr)
        ready.set()
        try:
            while not stop.is_set():
                if not sock.poll(50):
                    continue
                # Measure only the actual recv() syscall, not idle poll time.
                t0 = time.perf_counter_ns()
                raw = sock.recv(copy=True)
                wait_us = (time.perf_counter_ns() - t0) / 1e3

                ts_ns = struct.unpack("!Q", raw[:8])[0]
                d0 = time.perf_counter_ns()
                e2e_ms = (d0 - ts_ns) / 1e6
                dec_us = (time.perf_counter_ns() - d0) / 1e3

                cb0 = time.perf_counter_ns()
                metrics.add_recv(e2e_ms, wait_us, dec_us, (time.perf_counter_ns() - cb0) / 1e3)
        finally:
            sock.close(linger=0)

    def sender():
        sock = ctx.socket(zmq.PUSH)
        sock.setsockopt(zmq.SNDHWM, 100_000)
        sock.connect(addr)
        while not stop.is_set():
            ts = time.perf_counter_ns()
            msg = struct.pack("!Q", ts) + body
            enc_us = (time.perf_counter_ns() - ts) / 1e3
            t1 = time.perf_counter_ns()
            try:
                sock.send(msg, copy=True)
            except Exception:
                continue
            metrics.add_send(enc_us, (time.perf_counter_ns() - t1) / 1e3)
        sock.close(linger=0)

    recv_th = threading.Thread(target=receiver, daemon=True)
    recv_th.start()
    if not ready.wait(5):
        raise RuntimeError("pyzmq receiver not ready")
    time.sleep(warmup_s)

    snd_threads = [threading.Thread(target=sender, daemon=True) for _ in range(threads)]
    t0 = time.time()
    for th in snd_threads:
        th.start()
    time.sleep(duration_s)
    stop.set()
    for th in snd_threads:
        th.join(timeout=2)
    recv_th.join(timeout=2)
    elapsed = max(time.time() - t0, 1e-9)
    ctx.term()
    return metrics.to_result("pyzmq", payload_bytes, threads, elapsed)


def run_mooncake(payload_bytes, threads, duration_s, port, warmup_s):
    ZmqConfig, ZmqInterface, ZmqSocketOption, ZmqSocketType = load_mooncake()
    metrics = Metrics()
    stop = threading.Event()
    ready = threading.Event()
    body = b"x" * max(payload_bytes - 8, 0)

    def on_msg(msg):
        raw = msg["data"]
        if isinstance(raw, str):
            raw = raw.encode()
        ts_ns = struct.unpack("!Q", raw[:8])[0]
        d0 = time.perf_counter_ns()
        e2e_ms = (d0 - ts_ns) / 1e6
        dec_us = (time.perf_counter_ns() - d0) / 1e3
        cb0 = time.perf_counter_ns()
        metrics.add_recv(e2e_ms, 0.0, dec_us, (time.perf_counter_ns() - cb0) / 1e3)

    def receiver():
        iface = ZmqInterface()
        cfg = ZmqConfig()
        cfg.thread_count = max(threads, 2)
        cfg.pool_size = max(threads * 2, 4)
        iface.initialize(cfg)
        sock = iface.socket(ZmqSocketType.PULL)
        sock.bind(f"0.0.0.0:{port}")
        sock.start_server()
        sock.setsockopt(ZmqSocketOption.RCVTIMEO, 50)
        sock.set_pull_callback(on_msg)
        ready.set()
        try:
            while not stop.is_set():
                time.sleep(0.01)
        finally:
            time.sleep(0.2)
            sock.close()
            iface.shutdown()

    def sender():
        iface = ZmqInterface()
        cfg = ZmqConfig()
        cfg.thread_count = 2
        cfg.pool_size = 4
        iface.initialize(cfg)
        sock = iface.socket(ZmqSocketType.PUSH)
        sock.connect(f"127.0.0.1:{port}")
        while not stop.is_set():
            ts = time.perf_counter_ns()
            msg = struct.pack("!Q", ts) + body
            enc_us = (time.perf_counter_ns() - ts) / 1e3
            t1 = time.perf_counter_ns()
            if sock.push(msg) == 0:
                metrics.add_send(enc_us, (time.perf_counter_ns() - t1) / 1e3)
        sock.close()
        iface.shutdown()

    recv_th = threading.Thread(target=receiver, daemon=True)
    recv_th.start()
    if not ready.wait(5):
        raise RuntimeError("mooncake receiver not ready")
    time.sleep(warmup_s)

    snd_threads = [threading.Thread(target=sender, daemon=True) for _ in range(threads)]
    t0 = time.time()
    for th in snd_threads:
        th.start()
    time.sleep(duration_s)
    stop.set()
    for th in snd_threads:
        th.join(timeout=2)
    recv_th.join(timeout=3)
    elapsed = max(time.time() - t0, 1e-9)
    return metrics.to_result(
        "mooncake", payload_bytes, threads, elapsed,
        notes="recv_wait not observable via callback path (always 0)",
    )


def analyze_gap(mc: Result | None, pz: Result | None) -> dict:
    if mc is None or pz is None:
        return {"available": False}
    deltas = {
        "encode_us":   mc.encode_avg_us   - pz.encode_avg_us,
        "send_api_us": mc.send_api_avg_us - pz.send_api_avg_us,
        "decode_us":   mc.decode_avg_us   - pz.decode_avg_us,
        "callback_us": mc.callback_avg_us - pz.callback_avg_us,
    }
    bottleneck = max(deltas, key=deltas.get)
    reasons = {
        "encode_us":   "Python encode overhead",
        "send_api_us": "send / Python-to-backend boundary",
        "decode_us":   "decode path",
        "callback_us": "receiver callback",
    }
    pct = lambda a, b: (a - b) * 100 / b if b > 1e-12 else 0.0
    return {
        "available": True,
        "throughput_gap_pct": pct(mc.msg_s, pz.msg_s),
        "latency_p95_gap_pct": pct(mc.e2e_p95_ms, pz.e2e_p95_ms),
        "phase_deltas_us": deltas,
        "bottleneck": bottleneck,
        "inference": reasons[bottleneck],
    }


def print_result(r: Result):
    print(
        f"[{r.backend}] {r.payload_kb}KB × {r.threads}t  "
        f"msg/s={r.msg_s:,.0f}  MB/s={r.mb_s:.2f}  "
        f"p50={r.e2e_p50_ms:.3f}ms  p95={r.e2e_p95_ms:.3f}ms"
    )
    total = max(r.encode_avg_us + r.send_api_avg_us + r.recv_wait_avg_us
                + r.decode_avg_us + r.callback_avg_us, 1e-9)
    for name, val in [
        ("encode",    r.encode_avg_us),
        ("send_api",  r.send_api_avg_us),
        ("recv_wait", r.recv_wait_avg_us),
        ("decode",    r.decode_avg_us),
        ("callback",  r.callback_avg_us),
    ]:
        print(f"  {name:<10} {val:8.2f} us  ({100 * val / total:.1f}%)")
    if r.notes:
        print(f"  note: {r.notes}")


def print_gap(gap: dict):
    if not gap.get("available"):
        print("\n[Gap] N/A (need both backends)")
        return
    print(
        f"\n[Gap] throughput {gap['throughput_gap_pct']:+.1f}%  "
        f"latency-p95 {gap['latency_p95_gap_pct']:+.1f}%"
    )
    for k, v in gap["phase_deltas_us"].items():
        print(f"  Δ{k:<14} {v:+.2f} us")
    print(f"  bottleneck: {gap['bottleneck']}  →  {gap['inference']}")


def main():
    if "GLOG_minloglevel" not in os.environ:
        os.environ["GLOG_minloglevel"] = "3"

    p = argparse.ArgumentParser(description="ZMQ benchmark: mooncake vs pyzmq")
    p.add_argument("--backend", choices=["pyzmq", "mooncake", "both"], default="both")
    p.add_argument("--payload-kb", type=int, default=4)
    p.add_argument("--payloads", type=str, default="", help="comma-separated KB list, e.g. 4,64,1024")
    p.add_argument("--threads", type=int, default=1)
    p.add_argument("--duration", type=float, default=1.5, help="benchmark duration in seconds")
    p.add_argument("--warmup", type=float, default=0.3, help="warmup seconds before sampling")
    p.add_argument("--runs", type=int, default=1, help="repeat count per payload size")
    p.add_argument("--port", type=int, default=64000)
    p.add_argument("--json", action="store_true", help="print JSON report to stdout")
    p.add_argument("--out", type=str, default="", help="save JSON report to file")
    args = p.parse_args()

    payloads_kb = (
        [int(x) for x in args.payloads.split(",") if x.strip()]
        if args.payloads.strip()
        else [args.payload_kb]
    )
    backends = ["pyzmq", "mooncake"] if args.backend == "both" else [args.backend]
    run_fns = {"pyzmq": run_pyzmq, "mooncake": run_mooncake}

    all_results, all_gaps = [], []
    port = args.port
    for kb in payloads_kb:
        for run_idx in range(max(args.runs, 1)):
            by_backend: dict[str, Result] = {}
            for i, backend in enumerate(backends):
                try:
                    r = run_fns[backend](kb * 1024, args.threads, args.duration, port + i, args.warmup)
                    by_backend[backend] = r
                    all_results.append(r)
                    print_result(r)
                except Exception as e:
                    print(f"[{backend}] error: {e}")
            port += len(backends)
            gap = analyze_gap(by_backend.get("mooncake"), by_backend.get("pyzmq"))
            all_gaps.append({"payload_kb": kb, "run": run_idx + 1, "gap": gap})
            print_gap(gap)
            time.sleep(0.2)

    if args.json or args.out:
        report = {"results": [asdict(r) for r in all_results], "gaps": all_gaps}
        if args.json:
            print(json.dumps(report, indent=2))
        if args.out:
            with open(args.out, "w") as f:
                json.dump(report, f, indent=2)
            print(f"[saved] {args.out}")


if __name__ == "__main__":
    main()
