#!/usr/bin/env python3
# Copyright 2026 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Benchmark async batch transfers and non-blocking status polling in Python.

Suites (`--suite {all,throughput,poller,overlap,poll_cost}`):
  1. throughput: sync_list vs sync_arrays vs blocking get_batch_transfer_status
                 vs non-blocking transfer_check_status vs pipelined batches.
  2. poller:     concurrent producer + background poller thread scalability
                 (full scan vs windowed check_status vs vectorized
                 batch_transfer_check_status vs coalesced array batches).
  3. overlap:    interleaving host CPU compute with non-blocking check_status.
  4. poll_cost:  in-flight (status == 0) per-call latency across batch sizes.

Examples:
  python3 mooncake-transfer-engine/example/batch_async_arrays_bench.py \
      --mode loopback --protocol rdma --device_name mlx5_0 --hugepages
  python3 mooncake-transfer-engine/example/batch_async_arrays_bench.py \
      --mode loopback --protocol rdma --device_name mlx5_0 --hugepages \
      --suite poller --inflight_transfers 32768 --min_inflight_ms 2000
"""

from __future__ import annotations

import argparse
from array import array
from collections import OrderedDict, deque
from concurrent.futures import Future
import ctypes
import ctypes.util
import hashlib
import itertools
import json
import os
import signal
import statistics
import subprocess
import sys
from threading import Condition, RLock, Thread
import time
from typing import Any, Callable

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]


def _import_engine() -> tuple[Any, Any]:
    try:
        from mooncake.engine import TransferEngine, TransferOpcode

        return TransferEngine, TransferOpcode
    except ImportError:
        from engine import TransferEngine, TransferOpcode  # type: ignore[import-not-found]

        return TransferEngine, TransferOpcode


def u64_buf(values: list[int]) -> Any:
    return np.asarray(values, dtype=np.uint64) if np is not None else array("Q", values)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--mode", choices=["loopback", "target", "initiator"], default="loopback"
    )
    p.add_argument(
        "--suite",
        choices=["all", "throughput", "poller", "overlap", "poll_cost"],
        default="all",
    )
    p.add_argument("--local_server_name", default="127.0.0.1:12345")
    p.add_argument("--target_server_name", default="")
    p.add_argument("--metadata_server", default="P2PHANDSHAKE")
    p.add_argument("--protocol", default="tcp")
    p.add_argument("--device_name", default="")
    p.add_argument("--target_device_name", default="")
    p.add_argument("--opcode", choices=["read", "write"], default="read")
    p.add_argument("--pool_size_mb", type=int, default=512)
    p.add_argument("--block_size", type=int, default=16384)
    p.add_argument("--stride_bytes", type=int, default=32768)
    p.add_argument("--batch_sizes", default="64,512,2048")
    p.add_argument("--pipeline_depth", type=int, default=4)
    p.add_argument("--inflight_transfers", type=int, default=8192)
    p.add_argument("--poll_window", type=int, default=256)
    p.add_argument("--coalesced_batches", type=int, default=1)
    p.add_argument("--min_inflight_ms", type=float, default=0.0)
    p.add_argument("--poll_batch_sizes", default="64,512,2048,4096")
    p.add_argument("--poll_block_size", type=int, default=65536)
    p.add_argument("--compute_us", type=float, default=0.0)
    p.add_argument("--iterations", type=int, default=20)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument("--hugepages", action="store_true")
    p.add_argument("--use_gpu", action="store_true")
    p.add_argument("--gpu_id", type=int, default=0)
    p.add_argument("--output", default="")
    return p.parse_args()


class MemoryPool:
    def __init__(
        self, size: int, *, use_gpu: bool, gpu_id: int, hugepages: bool
    ) -> None:
        self.size = size
        self._libc: Any = None
        self._tensor: Any = None
        if use_gpu:
            import torch

            torch.cuda.set_device(gpu_id)
            self._tensor = torch.zeros(size, dtype=torch.uint8, device=f"cuda:{gpu_id}")
            self.addr = int(self._tensor.data_ptr())
            return

        libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
        libc.mmap.restype = ctypes.c_void_p
        libc.mmap.argtypes = [
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_long,
        ]
        libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self._libc = libc
        prot, flags, failed = 0x3, 0x22, ctypes.c_void_p(-1).value
        ptr = libc.mmap(None, size, prot, flags | 0x40000, -1, 0) if hugepages else None
        if not ptr or ptr == failed:
            ptr = libc.mmap(None, size, prot, flags, -1, 0)
        if not ptr or ptr == failed:
            raise RuntimeError(f"mmap({size}) failed: errno={ctypes.get_errno()}")
        ctypes.memset(ptr, 0, size)
        self.addr = int(ptr)

    def close(self) -> None:
        if getattr(self, "addr", 0) and self._libc is not None:
            self._libc.munmap(ctypes.c_void_p(self.addr), self.size)
            self.addr = 0
        self._tensor = None


def poll_done(engine: Any, bid: int) -> int:
    while (rc := engine.transfer_check_status(bid)) == 0:
        pass
    if rc != 1:
        raise RuntimeError(f"transfer_check_status({bid}) failed with {rc}")
    return rc


def strided_batch(
    lbase: int,
    rbase: int,
    pool_bytes: int,
    count: int,
    block: int,
    stride: int,
    slot: int = 0,
) -> tuple[list[int], list[int], list[int], Any, Any, Any]:
    step = max(block, stride)
    usable = pool_bytes - block
    shift = (slot * count * step) % usable
    ls = [lbase + ((shift + i * step) % usable) for i in range(count)]
    rs = [rbase + ((shift + i * step) % usable) for i in range(count)]
    ns = [block] * count
    return ls, rs, ns, u64_buf(ls), u64_buf(rs), u64_buf(ns)


def bench_us(
    fn: Callable[[], None], warmup: int, iters: int, nbytes: int
) -> dict[str, float]:
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(iters):
        t0 = time.perf_counter_ns()
        fn()
        samples.append((time.perf_counter_ns() - t0) / 1000.0)
    samples.sort()
    p50 = samples[len(samples) // 2]
    p99 = samples[min(len(samples) - 1, int(len(samples) * 0.99))]
    return {
        "avg_us": round(statistics.mean(samples), 2),
        "p50_us": round(p50, 2),
        "p99_us": round(p99, 2),
        "gbps": round((nbytes / 1e9) / (p50 / 1e6), 3) if p50 > 0 else 0.0,
    }


def verify_sanity(
    engine: Any, peer: str, lbase: int, rbase: int, pool_bytes: int, use_gpu: bool
) -> None:
    if engine.transfer_check_status(0) != -1 or engine.batch_transfer_check_status(
        [0]
    ) != [-1]:
        raise RuntimeError("Invalid batch_id=0 guard check failed")
    empty = u64_buf([])
    poll_done(
        engine, engine.batch_transfer_async_write_arrays(peer, empty, empty, empty)
    )
    if use_gpu or pool_bytes < 64 * 8192:
        return
    expected, bids = [], []
    for b in range(2):
        ws = [lbase + (b * 16 + i) * 8192 for i in range(16)]
        ds = [rbase + (b * 16 + i) * 8192 for i in range(16)]
        for i, addr in enumerate(ws):
            data = (f"b{b}-{i:02d}-".encode() * 700)[:4096]
            expected.append(data)
            engine.write_bytes_to_buffer(addr, data, 4096)
        bids.append(
            engine.batch_transfer_async_write_arrays(
                peer, u64_buf(ws), u64_buf(ds), u64_buf([4096] * 16)
            )
        )
    pending = list(bids)
    while pending:
        pending = [
            b
            for b, s in zip(pending, engine.batch_transfer_check_status(pending))
            if s == 0
        ]
    rds = [lbase + (32 + i) * 8192 for i in range(32)]
    for addr in rds:
        engine.write_bytes_to_buffer(addr, b"\x00" * 4096, 4096)
    rbid = engine.batch_transfer_async_read_arrays(
        peer,
        u64_buf(rds),
        u64_buf([rbase + i * 8192 for i in range(32)]),
        u64_buf([4096] * 32),
    )
    poll_done(engine, rbid)
    for i, addr in enumerate(rds):
        if engine.read_bytes_from_buffer(addr, 4096) != expected[i]:
            raise RuntimeError(f"Round-trip data mismatch at chunk {i}")


def run_throughput_suite(
    engine: Any,
    peer: str,
    lbase: int,
    rbase: int,
    pool_bytes: int,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    is_read = args.opcode == "read"
    sync_l = (
        engine.batch_transfer_sync_read if is_read else engine.batch_transfer_sync_write
    )
    sync_a = (
        engine.batch_transfer_sync_read_arrays
        if is_read
        else engine.batch_transfer_sync_write_arrays
    )
    async_a = (
        engine.batch_transfer_async_read_arrays
        if is_read
        else engine.batch_transfer_async_write_arrays
    )

    print(
        f"\n{'=' * 102}\n[Suite 1: Throughput & Completion Modes] {args.opcode.upper()} block={args.block_size}B\n{'=' * 102}"
    )
    print(
        f"{'Batch':>7}  {'Method':<44}  {'p50(us)':>9}  {'avg(us)':>9}  {'p99(us)':>9}  {'GB/s':>7}  {'Speedup':>8}\n{'-' * 102}"
    )
    out = []
    for bsz in [int(x) for x in args.batch_sizes.split(",") if x.strip()]:
        nbytes = bsz * args.block_size
        ls, rs, ns, la, ra, na = strided_batch(
            lbase, rbase, pool_bytes, bsz, args.block_size, args.stride_bytes
        )
        methods: dict[str, dict[str, float]] = {}

        def _sync_list() -> None:
            if sync_l(peer, ls, rs, ns) != 0:
                raise RuntimeError("sync_list failed")

        def _sync_arr() -> None:
            if sync_a(peer, la, ra, na) != 0:
                raise RuntimeError("sync_arrays failed")

        def _async_block() -> None:
            bid = async_a(peer, la, ra, na)
            if bid == 0 or engine.get_batch_transfer_status([bid]) != 0:
                raise RuntimeError("get_batch_transfer_status failed")

        def _async_poll() -> None:
            poll_done(engine, async_a(peer, la, ra, na))

        methods["1. sync_list"] = bench_us(
            _sync_list, args.warmup, args.iterations, nbytes
        )
        methods["2. sync_arrays"] = bench_us(
            _sync_arr, args.warmup, args.iterations, nbytes
        )
        methods["3. async_arrays + get_batch_status (block)"] = bench_us(
            _async_block, args.warmup, args.iterations, nbytes
        )
        methods["4. async_arrays + transfer_check_status"] = bench_us(
            _async_poll, args.warmup, args.iterations, nbytes
        )

        depth = max(2, args.pipeline_depth)
        slots = [
            strided_batch(
                lbase, rbase, pool_bytes, bsz, args.block_size, args.stride_bytes, s
            )[3:]
            for s in range(depth)
        ]
        total_b = max(depth * 4, args.iterations)
        t0, inflight, free_s, sent = time.perf_counter_ns(), [], deque(range(depth)), 0
        while sent < total_b or inflight:
            while sent < total_b and free_s:
                s = free_s.popleft()
                inflight.append((async_a(peer, *slots[s]), s))
                sent += 1
            statuses = engine.batch_transfer_check_status([b for b, _ in inflight])
            remaining = []
            for (b, s), st in zip(inflight, statuses):
                if st == 0:
                    remaining.append((b, s))
                elif st == 1:
                    free_s.append(s)
                else:
                    raise RuntimeError(f"pipelined batch failed: {st}")
            inflight = remaining
        am_us = ((time.perf_counter_ns() - t0) / 1000.0) / total_b
        methods[f"5. async_arrays + vec_check_status (pipe={depth})"] = {
            "avg_us": round(am_us, 2),
            "p50_us": round(am_us, 2),
            "p99_us": round(am_us, 2),
            "gbps": round((nbytes / 1e9) / (am_us / 1e6), 3),
        }

        base_us = methods["3. async_arrays + get_batch_status (block)"]["p50_us"]
        for name, st in methods.items():
            print(
                f"{bsz:>7}  {name:<44}  {st['p50_us']:>9.1f}  {st['avg_us']:>9.1f}  {st['p99_us']:>9.1f}  {st['gbps']:>7.2f}  {base_us / st['p50_us']:>7.2f}x"
            )
        print("-" * 102)
        out.append({"batch_size": bsz, "total_bytes": nbytes, "methods": methods})
    return out


class _PollerProxy:
    def __init__(self, engine: Any, hold_s: float) -> None:
        self._eng, self._hold_s = engine, hold_s
        self._ready: dict[int, float] = {}
        self.pybind_polls = self.items_checked = 0

    def reset(self) -> None:
        self.pybind_polls = self.items_checked = 0
        self._ready.clear()

    def _track(self, bid: int) -> int:
        if bid != 0 and self._hold_s > 0:
            self._ready[bid] = time.perf_counter() + self._hold_s
        return bid

    def submit_one(self, peer: str, laddr: int, raddr: int, sz: int, op: Any) -> int:
        return self._track(
            self._eng.batch_transfer_async(peer, [laddr], [raddr], [sz], op)
        )

    def submit_arr(self, peer: str, la: Any, ra: Any, na: Any, is_read: bool) -> int:
        fn = (
            self._eng.batch_transfer_async_read_arrays
            if is_read
            else self._eng.batch_transfer_async_write_arrays
        )
        return self._track(fn(peer, la, ra, na))

    def check_one(self, bid: int) -> int:
        self.pybind_polls += 1
        self.items_checked += 1
        if self._hold_s > 0 and time.perf_counter() < self._ready.get(bid, 0.0):
            self._eng.transfer_check_status(0)
            return 0
        self._ready.pop(bid, None)
        return self._eng.transfer_check_status(bid)

    def check_many(self, bids: list[int]) -> list[int]:
        self.pybind_polls += 1
        self.items_checked += len(bids)
        if self._hold_s <= 0:
            return self._eng.batch_transfer_check_status(bids)
        now = time.perf_counter()
        held = [now < self._ready.get(b, 0.0) for b in bids]
        raw = self._eng.batch_transfer_check_status(
            [0 if h else b for b, h in zip(bids, held)]
        )
        for b, h, s in zip(bids, held, raw):
            if not h and s != 0:
                self._ready.pop(b, None)
        return [0 if h else s for h, s in zip(held, raw)]


class _AsyncPoller:
    def __init__(
        self, proxy: _PollerProxy, window: int | None, idle_wake: bool, vec: bool
    ) -> None:
        self._p, self._win, self._idle, self._vec = proxy, window, idle_wake, vec
        self._lock, self._cv = RLock(), Condition()
        self._pending: dict[int, Future[None]] = {}
        self._batches: OrderedDict[int, Future[None]] = OrderedDict()
        self._thread: Thread | None = None
        self._stop = False
        self._seq = 0
        self.peak = 0

    def _add(self, bid: int) -> Future[None]:
        if bid == 0:
            raise RuntimeError("async submit returned 0")
        fut: Future[None] = Future()
        self._seq += 1
        work_id = self._seq
        self._pending[work_id] = fut
        fut.add_done_callback(lambda _, k=work_id: self._prune(k))
        with self._cv:
            if not self._idle or not self._batches:
                self._cv.notify()
            self._batches[bid] = fut
            self.peak = max(self.peak, len(self._batches))
            if self._thread is None:
                self._thread = Thread(target=self._run, daemon=True)
                self._thread.start()
        return fut

    def _prune(self, work_id: int) -> None:
        with self._lock:
            self._pending.pop(work_id, None)

    def submit_one(
        self, peer: str, laddr: int, raddr: int, sz: int, op: Any
    ) -> Future[None]:
        with self._lock:
            return self._add(self._p.submit_one(peer, laddr, raddr, sz, op))

    def submit_arr(
        self, peer: str, la: Any, ra: Any, na: Any, is_read: bool
    ) -> Future[None]:
        with self._lock:
            return self._add(self._p.submit_arr(peer, la, ra, na, is_read))

    def _run(self) -> None:
        delay = 0.0
        while True:
            with self._cv:
                if delay:
                    self._cv.wait(delay)
                while not self._batches and not self._stop:
                    self._cv.wait()
                    delay = 0.0
                if self._stop:
                    return
                items = (
                    list(self._batches.items())
                    if self._win is None
                    else list(itertools.islice(self._batches.items(), self._win))
                )
                statuses = (
                    self._p.check_many([b for b, _ in items])
                    if self._vec
                    else [self._p.check_one(b) for b, _ in items]
                )
            settled = [(b, f, s) for (b, f), s in zip(items, statuses) if s != 0]
            rot = (
                [b for (b, _), s in zip(items, statuses) if s == 0]
                if self._win is not None
                else []
            )
            with self._cv:
                for b in rot:
                    self._batches.move_to_end(b)
                for b, f, _ in settled:
                    if self._batches.get(b) is f:
                        del self._batches[b]
            for _, f, s in settled:
                if s == 1:
                    f.set_result(None)
                else:
                    f.set_exception(RuntimeError(f"transfer failed ({s})"))
            delay = 0.0 if settled else min(max(delay * 2, 1e-6), 1e-3)

    def close(self) -> None:
        with self._cv:
            self._stop = True
            self._cv.notify()
        if self._thread is not None:
            self._thread.join(timeout=5.0)


def run_poller_suite(
    engine: Any,
    opcode_cls: Any,
    peer: str,
    lbase: int,
    rbase: int,
    pool_bytes: int,
    args: argparse.Namespace,
) -> dict[str, Any]:
    is_read = args.opcode == "read"
    op = opcode_cls.Read if is_read else opcode_cls.Write
    n, blk, win = args.inflight_transfers, args.block_size, args.poll_window
    ls, rs, ns, _, _, _ = strided_batch(
        lbase, rbase, pool_bytes, n, blk, args.stride_bytes
    )
    ncoal = max(1, min(args.coalesced_batches, n))
    step = (n + ncoal - 1) // ncoal
    coals = [
        (
            u64_buf(ls[s : s + step]),
            u64_buf(rs[s : s + step]),
            u64_buf(ns[s : s + step]),
        )
        for s in range(0, n, step)
    ]

    rounds = max(1, min(args.iterations, 5))
    proxy = _PollerProxy(engine, args.min_inflight_ms / 1000.0)
    nbytes = n * blk
    print(
        f"\n{'=' * 118}\n[Suite 2: Submitter + Background Poller] {n} x {blk // 1024} KiB ({nbytes / 1048576:.1f} MiB), min_inflight={args.min_inflight_ms:.0f} ms\n{'=' * 118}"
    )
    print(
        f"{'Design':<52}  {'Submit(ms)':>10}  {'Total(ms)':>10}  {'PybindPolls':>12}  {'ItemsChkd':>11}  {'PeakPend':>8}  {'GB/s':>6}\n{'-' * 118}"
    )

    cases: dict[str, Any] = {}
    for label, w_sz, idle, vec, arr in [
        ("1. per_item + full_scan check_status", None, False, False, False),
        (f"2. per_item + windowed check_status (w={win})", win, True, False, False),
        (
            f"3. per_item + batch_transfer_check_status (w={win})",
            win,
            True,
            True,
            False,
        ),
        (
            f"4. coalesced_arrays + check_status (batches={len(coals)})",
            win,
            True,
            len(coals) > 1,
            True,
        ),
    ]:
        runs = []
        for _ in range(rounds):
            proxy.reset()
            poller = _AsyncPoller(proxy, w_sz, idle, vec)
            try:
                t0 = time.perf_counter()
                futs = (
                    [
                        poller.submit_arr(peer, la, ra, na, is_read)
                        for la, ra, na in coals
                    ]
                    if arr
                    else [
                        poller.submit_one(peer, ls[i], rs[i], ns[i], op)
                        for i in range(n)
                    ]
                )
                tsub = time.perf_counter()
                for f in futs:
                    f.result(timeout=120.0)
                tend = time.perf_counter()
            finally:
                poller.close()
            tot_ms = (tend - t0) * 1000.0
            runs.append(
                (
                    (tsub - t0) * 1000.0,
                    tot_ms,
                    proxy.pybind_polls,
                    proxy.items_checked,
                    poller.peak,
                    (nbytes / 1e9) / (tot_ms / 1000.0),
                )
            )
        med = [statistics.median([r[i] for r in runs]) for i in range(6)]
        cases[label] = {
            "submit_ms": round(med[0], 2),
            "total_ms": round(med[1], 2),
            "pybind_polls": int(round(med[2])),
            "items_checked": int(round(med[3])),
            "peak_inflight": int(round(med[4])),
            "gbps": round(med[5], 3),
        }
        print(
            f"{label:<52}  {med[0]:>10.1f}  {med[1]:>10.1f}  {int(round(med[2])):>12,d}  {int(round(med[3])):>11,d}  {int(round(med[4])):>8,d}  {med[5]:>6.2f}"
        )
    print("-" * 118)
    return {
        "inflight_transfers": n,
        "min_inflight_ms": args.min_inflight_ms,
        "cases": cases,
    }


def run_overlap_suite(
    engine: Any,
    peer: str,
    lbase: int,
    rbase: int,
    pool_bytes: int,
    args: argparse.Namespace,
) -> dict[str, Any]:
    bsz = [int(x) for x in args.batch_sizes.split(",") if x.strip()][-1]
    is_read = args.opcode == "read"
    sync_a = (
        engine.batch_transfer_sync_read_arrays
        if is_read
        else engine.batch_transfer_sync_write_arrays
    )
    async_a = (
        engine.batch_transfer_async_read_arrays
        if is_read
        else engine.batch_transfer_async_write_arrays
    )
    _, _, _, la, ra, na = strided_batch(
        lbase, rbase, pool_bytes, bsz, args.block_size, args.stride_bytes
    )

    xfer_us = bench_us(
        lambda: poll_done(engine, async_a(peer, la, ra, na)),
        1,
        max(4, args.warmup),
        bsz * args.block_size,
    )["p50_us"]
    target_us = args.compute_us if args.compute_us > 0 else max(100.0, xfer_us * 0.75)
    payload = b"mooncake-overlap-work" * 256
    t0 = time.perf_counter_ns()
    for _ in range(200):
        hashlib.sha256(payload).digest()
    per_chunk = max(
        1,
        int(
            round(
                (target_us * 1000.0) / max(1.0, (time.perf_counter_ns() - t0) / 200.0)
            )
        )
        // 16,
    )

    def do_work(poll_cb: Callable[[], None] | None = None) -> None:
        for _ in range(16):
            for _ in range(per_chunk):
                hashlib.sha256(payload).digest()
            if poll_cb is not None:
                poll_cb()

    work_us = bench_us(do_work, 1, 8, 0)["p50_us"]
    s_sync = bench_us(
        lambda: (sync_a(peer, la, ra, na), do_work()), 1, args.iterations, 0
    )["p50_us"]
    s_blk = bench_us(
        lambda: (
            engine.get_batch_transfer_status([async_a(peer, la, ra, na)]),
            do_work(),
        ),
        1,
        args.iterations,
        0,
    )["p50_us"]

    ovlp_samples, early = [], 0
    for _ in range(args.iterations):
        t0 = time.perf_counter_ns()
        bid, done = async_a(peer, la, ra, na), False

        def _tick() -> None:
            nonlocal done
            if not done and (rc := engine.transfer_check_status(bid)) != 0:
                if rc != 1:
                    raise RuntimeError(f"overlap transfer failed: {rc}")
                done = True

        do_work(_tick)
        if done:
            early += 1
        else:
            poll_done(engine, bid)
        ovlp_samples.append((time.perf_counter_ns() - t0) / 1000.0)
    s_ovlp = statistics.median(ovlp_samples)

    print(
        f"\n{'=' * 92}\n[Suite 3: Compute + Transfer Overlap] batch={bsz}, xfer_p50={xfer_us:.1f} us, work_p50={work_us:.1f} us\n{'=' * 92}"
    )
    for lbl, val in [
        ("1. sync_arrays -> host_work (serial)", s_sync),
        ("2. async_arrays -> get_batch_status -> host_work", s_blk),
        ("3. async_arrays + transfer_check_status (overlap)", s_ovlp),
    ]:
        print(f"{lbl:<50}  {val:>12.1f} us  {s_sync / val:>9.2f}x")
    print(f"  -> Completed during host compute in {early}/{args.iterations} steps")
    return {
        "batch_size": bsz,
        "serial_us": round(s_sync, 2),
        "blocking_us": round(s_blk, 2),
        "overlap_us": round(s_ovlp, 2),
    }


def run_poll_cost_suite(
    engine: Any,
    peer: str,
    lbase: int,
    rbase: int,
    pool_bytes: int,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    async_a = (
        engine.batch_transfer_async_read_arrays
        if args.opcode == "read"
        else engine.batch_transfer_async_write_arrays
    )
    print(
        f"\n{'=' * 92}\n[Suite 4: In-Flight Poll Cost] block={args.poll_block_size}B (transfer_check_status while status == 0)\n{'=' * 92}"
    )
    print(
        f"{'Batch Size':>12}  {'Waiting Polls':>15}  {'Poll p50 (us)':>15}  {'Poll avg (us)':>15}  {'Terminal (us)':>15}\n{'-' * 92}"
    )
    out = []
    for bsz in [int(x) for x in args.poll_batch_sizes.split(",") if x.strip()]:
        _, _, _, la, ra, na = strided_batch(
            lbase,
            rbase,
            pool_bytes,
            bsz,
            args.poll_block_size,
            max(args.poll_block_size, args.stride_bytes),
        )
        wait_ns: list[int] = []
        term_ns: list[int] = []
        for _ in range(max(8, min(args.iterations, 25))):
            bid = async_a(peer, la, ra, na)
            while True:
                t0 = time.perf_counter_ns()
                rc = engine.transfer_check_status(bid)
                dt = time.perf_counter_ns() - t0
                if rc == 0:
                    wait_ns.append(dt)
                elif rc == 1:
                    term_ns.append(dt)
                    break
                else:
                    raise RuntimeError(f"poll_cost failed: {rc}")
        wp50 = statistics.median(wait_ns) / 1000.0 if wait_ns else 0.0
        wavg = statistics.mean(wait_ns) / 1000.0 if wait_ns else 0.0
        tp50 = statistics.median(term_ns) / 1000.0 if term_ns else 0.0
        print(
            f"{bsz:>12}  {len(wait_ns):>15}  {wp50:>15.3f}  {wavg:>15.3f}  {tp50:>15.3f}"
        )
        out.append(
            {
                "batch_size": bsz,
                "waiting_polls": len(wait_ns),
                "poll_p50_us": round(wp50, 3),
                "poll_avg_us": round(wavg, 3),
            }
        )
    return out


def _server_name(local_name: str, meta: str, engine: Any) -> str:
    if meta == "P2PHANDSHAKE":
        return f"{local_name.rpartition(':')[0] or local_name}:{engine.get_rpc_port()}"
    return local_name


def run_target(args: argparse.Namespace) -> None:
    TransferEngine, _ = _import_engine()
    pool_bytes = args.pool_size_mb * 1048576
    pool = MemoryPool(
        pool_bytes, use_gpu=args.use_gpu, gpu_id=args.gpu_id, hugepages=args.hugepages
    )
    engine = TransferEngine()
    if (
        engine.initialize(
            args.local_server_name,
            args.metadata_server,
            args.protocol,
            args.device_name,
        )
        != 0
    ):
        pool.close()
        raise RuntimeError("Target initialize failed")
    if engine.register_memory(pool.addr, pool_bytes) != 0:
        pool.close()
        raise RuntimeError("Target register_memory failed")
    print(
        f"TARGET_READY:{json.dumps({'server_name': _server_name(args.local_server_name, args.metadata_server, engine)})}",
        flush=True,
    )
    stop = False

    def _stop_handler(_s: int, _f: Any) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)
    try:
        while not stop:
            time.sleep(0.2)
    finally:
        engine.unregister_memory(pool.addr)
        pool.close()


def run_initiator(args: argparse.Namespace, peer: str) -> None:
    TransferEngine, TransferOpcode = _import_engine()
    pool_bytes = args.pool_size_mb * 1048576
    pool = MemoryPool(
        pool_bytes, use_gpu=args.use_gpu, gpu_id=args.gpu_id, hugepages=args.hugepages
    )
    engine = TransferEngine()
    if (
        engine.initialize(
            args.local_server_name,
            args.metadata_server,
            args.protocol,
            args.device_name,
        )
        != 0
    ):
        pool.close()
        raise RuntimeError("Initiator initialize failed")
    if engine.register_memory(pool.addr, pool_bytes) != 0:
        pool.close()
        raise RuntimeError("Initiator register_memory failed")
    try:
        rbase = engine.get_first_buffer_address(peer)
        if rbase == 0:
            raise RuntimeError(f"Failed to get remote buffer from {peer}")
        print(
            f"Connected to {peer} | protocol={args.protocol} | pool={args.pool_size_mb} MiB"
        )
        verify_sanity(engine, peer, pool.addr, rbase, pool_bytes, args.use_gpu)
        report: dict[str, Any] = {"protocol": args.protocol, "opcode": args.opcode}
        if args.suite in ("all", "throughput"):
            report["throughput"] = run_throughput_suite(
                engine, peer, pool.addr, rbase, pool_bytes, args
            )
        if args.suite in ("all", "poller"):
            report["poller"] = run_poller_suite(
                engine, TransferOpcode, peer, pool.addr, rbase, pool_bytes, args
            )
        if args.suite in ("all", "overlap"):
            report["overlap"] = run_overlap_suite(
                engine, peer, pool.addr, rbase, pool_bytes, args
            )
        if args.suite in ("all", "poll_cost"):
            report["poll_cost"] = run_poll_cost_suite(
                engine, peer, pool.addr, rbase, pool_bytes, args
            )
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
            print(f"\nSaved JSON report to {args.output}")
    finally:
        engine.unregister_memory(pool.addr)
        pool.close()


def run_loopback(args: argparse.Namespace) -> None:
    cmd = [
        sys.executable,
        os.path.abspath(__file__),
        "--mode=target",
        "--local_server_name=127.0.0.1",
        "--metadata_server=P2PHANDSHAKE",
        f"--protocol={args.protocol}",
        f"--pool_size_mb={args.pool_size_mb}",
    ]
    if tdev := (args.target_device_name or args.device_name):
        cmd.append(f"--device_name={tdev}")
    if args.hugepages:
        cmd.append("--hugepages")
    if args.use_gpu:
        cmd.extend(["--use_gpu", f"--gpu_id={args.gpu_id}"])
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
    )
    try:
        info, deadline = None, time.monotonic() + 30.0
        assert proc.stdout is not None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"Target exited ({proc.returncode}):\n{proc.stderr.read() if proc.stderr else ''}"
                )
            if (line := proc.stdout.readline()).startswith("TARGET_READY:"):
                info = json.loads(line.partition(":")[2])
                break
        if info is None:
            raise RuntimeError("Timed out waiting for local target subprocess")
        init_args = argparse.Namespace(**vars(args))
        init_args.local_server_name = "127.0.0.1"
        init_args.metadata_server = "P2PHANDSHAKE"
        run_initiator(init_args, info["server_name"])
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


def main() -> None:
    args = parse_args()
    if args.mode == "target":
        run_target(args)
    elif args.mode == "initiator":
        if not args.target_server_name:
            raise SystemExit("--target_server_name is required in initiator mode")
        run_initiator(args, args.target_server_name)
    else:
        run_loopback(args)


if __name__ == "__main__":
    main()
