#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Real-GPU check for native elastic connect, mask, and barrier contracts."""

import argparse
import socket
import threading
import time
from datetime import timedelta

import torch
import torch.distributed as dist
from mooncake.mooncake_ep_buffer import Buffer

CAPACITY, EXPERTS_PER_RANK = 4, 1
ACTIVE_RANKS, OLD_RANKS = (0, 1, 2), (0, 1)
TOKENS, HIDDEN, MAX_PREP_ROUNDS = 16, 2048, 128


class RecordingRuntimeProxy:
    """Forward all calls; time only the actual native `_stage_peer` call."""

    def __init__(self, runtime):
        self._runtime = runtime
        self._lock = threading.Lock()
        self.stage_started = threading.Event()
        self.intervals = []

    def __getattr__(self, name):
        return getattr(self._runtime, name)

    def _stage_peer(self, peer, *metadata):
        interval = {"peer": int(peer), "start_ns": time.monotonic_ns()}
        with self._lock:
            self.intervals.append(interval)
        self.stage_started.set()
        try:
            return self._runtime._stage_peer(peer, *metadata)
        finally:
            interval["end_ns"] = time.monotonic_ns()

    def reset(self):
        with self._lock:
            self.intervals.clear()
        self.stage_started.clear()

    def for_peer(self, peer):
        with self._lock:
            return [
                dict(x) for x in self.intervals if x["peer"] == peer and "end_ns" in x
            ]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rank", type=int, choices=ACTIVE_RANKS, required=True)
    parser.add_argument("--store-host", required=True)
    parser.add_argument("--store-port", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--local-gpu", type=int, default=0)
    parser.add_argument("--timeout-ms", type=int, default=1000)
    parser.add_argument("--control-timeout-s", type=float, default=180.0)
    parser.add_argument("--disable-ll-nvlink", action="store_true")
    return parser.parse_args()


def key(args, *parts):
    return "/".join(("P1_CONN", args.run_id, *(str(p) for p in parts)))


def wait(store, args, keys):
    store.wait(keys, timedelta(seconds=args.control_timeout_s))


def sync(store, args, name, rank, ranks):
    store.set(key(args, name, rank), b"ready")
    wait(store, args, [key(args, name, item) for item in ranks])


def make_store(args):
    return dist.TCPStore(
        host_name="0.0.0.0" if args.rank == 0 else args.store_host,
        port=args.store_port,
        is_master=args.rank == 0,
        wait_for_workers=False,
        timeout=timedelta(seconds=args.control_timeout_s),
    )


def check_mask(buffer, expected, label):
    mask = torch.empty(CAPACITY, dtype=torch.int32, device="cuda")
    buffer.query_mask_buffer(mask)
    torch.cuda.synchronize()
    actual = mask.cpu().tolist()
    assert actual == expected, f"{label}: expected {expected}, got {actual}"
    print(f"{label} rank={buffer.rank} mask={actual}", flush=True)


def run_round(buffer, rank, active_bound, label):
    device = torch.device("cuda", torch.cuda.current_device())
    values = torch.arange(TOKENS, dtype=torch.float32, device=device) + 1 + rank * 32
    x = values.to(torch.bfloat16).view(TOKENS, 1).expand(TOKENS, HIDDEN).contiguous()
    idx = (
        (torch.arange(TOKENS, device=device) % active_bound).to(torch.int64).view(-1, 1)
    )
    weights = torch.ones((TOKENS, 1), dtype=torch.float32, device=device)
    stats = torch.zeros(EXPERTS_PER_RANK, dtype=torch.int32, device=device)

    launch_ns = time.monotonic_ns()
    recv_x, recv_count, handle, event, _ = buffer.dispatch(
        x,
        idx,
        TOKENS,
        active_bound * EXPERTS_PER_RANK,
        use_fp8=False,
        cumulative_local_expert_recv_stats=stats,
        async_finish=True,
    )
    event.current_stream_wait()
    out = torch.empty_like(x)
    result, event, _ = buffer.combine(
        recv_x.clone(),
        idx,
        weights,
        handle,
        zero_copy=False,
        out=out,
        async_finish=True,
    )
    event.current_stream_wait()
    done = torch.cuda.Event()
    done.record(torch.cuda.current_stream())
    done.synchronize()
    completion_ns = time.monotonic_ns()
    assert torch.equal(stats, recv_count), f"{label}: recv stats/count mismatch"
    assert torch.equal(result, x), f"{label}: BF16 dispatch/combine mismatch"
    return launch_ns, completion_ns


def connect_worker(args, buffer, store, peers):
    status = b"ok"
    try:
        # CUDA current device is thread-local; native stage creates CUDA streams.
        torch.cuda.set_device(args.local_gpu)
        buffer.connect_ranks(peers, activate=False)
    except BaseException as exc:
        status = b"error"
        print(
            f"P1_STAGE_ERROR rank={args.rank}: {type(exc).__name__}: {exc}", flush=True
        )
    finally:
        store.set(key(args, "stage-done", args.rank), status)


def prepare_overlap(args, buffer, store, proxy, thread):
    deadline = time.monotonic() + args.control_timeout_s
    while not proxy.stage_started.wait(0.1):
        if store.check([key(args, "stage-done", args.rank)]):
            raise RuntimeError("connect_ranks returned without native stage")
        if time.monotonic() >= deadline:
            raise TimeoutError("native _stage_peer did not start")

    rounds = []
    for i in range(MAX_PREP_ROUNDS):
        launch_ns, done_ns = run_round(buffer, args.rank, 2, f"prepare-{i}")
        rounds.append((i, launch_ns, done_ns))
        sync(store, args, f"old-round-{i}", args.rank, OLD_RANKS)
        decision = key(args, "old-loop", i)
        if args.rank == 0:
            keys = [key(args, "stage-done", r) for r in ACTIVE_RANKS]
            store.set(decision, b"stop" if store.check(keys) else b"more")
        wait(store, args, [decision])
        if store.get(decision) == b"stop":
            break
    else:
        raise TimeoutError("native prepare exceeded the old-communication round bound")

    thread.join(args.control_timeout_s)
    assert not thread.is_alive(), "connect_ranks(activate=False) did not return"
    statuses = [store.get(key(args, "stage-done", r)) for r in ACTIVE_RANKS]
    assert all(status == b"ok" for status in statuses), f"stage status={statuses}"
    intervals = proxy.for_peer(2)
    assert len(intervals) == 1, f"expected one rank-2 native stage, got {intervals}"
    span = intervals[0]
    overlaps = [
        r for r in rounds if r[1] >= span["start_ns"] and r[2] <= span["end_ns"]
    ]
    assert (
        overlaps
    ), f"no full GPU round inside native stage: span={span} rounds={rounds}"
    i, launch_ns, done_ns = overlaps[0]
    print(
        f"P1_NATIVE_PREP_GPU_OVERLAP rank={args.rank} peer=2 round={i} "
        f"launch_ns={launch_ns} stage_begin_ns={span['start_ns']} "
        f"completion_ns={done_ns} stage_end_ns={span['end_ns']}",
        flush=True,
    )


def main():
    args = parse_args()
    if "/" in args.run_id:
        raise ValueError("--run-id must not contain '/' characters")
    if not torch.cuda.is_available():
        raise RuntimeError("this smoke requires a CUDA device")
    torch.cuda.set_device(args.local_gpu)
    store = make_store(args)
    rdma_bytes = Buffer.get_low_latency_buffer_size_hint(
        TOKENS, HIDDEN, CAPACITY, CAPACITY
    )
    buffer = Buffer.create_elastic(
        disable_ll_nvlink=args.disable_ll_nvlink,
        explicitly_destroy=True,
        rank=args.rank,
        tcp_store_group=store,
        timeout_ms=args.timeout_ms,
    )
    buffer.update_memory_buffers(CAPACITY, EXPERTS_PER_RANK, rdma_bytes)
    proxy = RecordingRuntimeProxy(buffer.runtime)
    buffer.runtime = proxy
    fast = bool(proxy.use_fast_path())
    if buffer._use_fallback or not fast:
        raise RuntimeError(
            f"native path required: fallback={buffer._use_fallback}, fast={fast}"
        )
    print(
        f"P1_NATIVE_ROUTE rank={args.rank} fallback=False use_fast_path={fast}",
        flush=True,
    )
    targets = ("_ep", "/mooncake/ep", "libmooncake_ep_device", "libcudart", "libcuda")
    mapped_paths = set()
    with open("/proc/self/maps", encoding="utf-8") as maps:
        for mapping in maps:
            path = mapping.rstrip().split(maxsplit=5)[-1]
            if path.startswith("/") and any(target in path for target in targets):
                mapped_paths.add(path)
    print(f"P1_NATIVE_MAPS rank={args.rank} paths={sorted(mapped_paths)}", flush=True)
    print(
        f"P1_CONNECTION_CONTRACT_START rank={args.rank} host={socket.gethostname()}",
        flush=True,
    )

    thread = None
    try:
        if args.rank in OLD_RANKS:
            peer = 1 - args.rank
            buffer.connect_ranks([peer], activate=False)
            sync(store, args, "old-staged", args.rank, OLD_RANKS)
            check_mask(
                buffer,
                [0 if r == args.rank else 1 for r in range(CAPACITY)],
                "P1_OLD_PRECOMMIT_MASK",
            )
            buffer.update_mask_buffer(peer, mask=False)
            sync(store, args, "old-committed", args.rank, OLD_RANKS)
            check_mask(buffer, [0, 0, 1, 1], "P1_OLD_COMMIT_MASK")
            # Warm one real round before timing prepare, so tensor allocation is not first-use overhead.
            run_round(buffer, args.rank, 2, "old-warmup")
            sync(store, args, "old-ready", args.rank, OLD_RANKS)
            print(f"P1_OLD_PAIR_READY rank={args.rank}", flush=True)
            wait(store, args, [key(args, "join-start")])
        else:
            wait(store, args, [key(args, "old-ready", r) for r in OLD_RANKS])
            store.set(key(args, "join-start"), b"rank-2-started")

        proxy.reset()
        peers = [2] if args.rank in OLD_RANKS else [0, 1]
        thread = threading.Thread(
            target=connect_worker, args=(args, buffer, store, peers)
        )
        thread.start()
        if args.rank in OLD_RANKS:
            prepare_overlap(args, buffer, store, proxy, thread)
        else:
            thread.join(args.control_timeout_s)
            assert (
                not thread.is_alive()
            ), "rank 2 connect_ranks(activate=False) timed out"
            assert store.get(key(args, "stage-done", 2)) == b"ok", "rank 2 stage failed"

        sync(store, args, "all-staged", args.rank, ACTIVE_RANKS)
        expected = [0, 0, 1, 1] if args.rank in OLD_RANKS else [1, 1, 0, 1]
        check_mask(buffer, expected, "P1_NEW_RANK_STILL_MASKED")

        # Recycle one prepared-only peer symmetrically at both endpoints.
        # Rank 1 joins each Store barrier but keeps its rank-2 stage intact.
        peer = 2 if args.rank == 0 else 0 if args.rank == 2 else None
        sync(store, args, "prepared-disconnect-start", args.rank, ACTIVE_RANKS)
        if peer is not None:
            buffer.disconnect_ranks([peer])
        sync(store, args, "prepared-disconnect-done", args.rank, ACTIVE_RANKS)
        check_mask(buffer, expected, "P1_PREPARED_DISCONNECT_MASK")

        sync(store, args, "prepared-reconnect-start", args.rank, ACTIVE_RANKS)
        if peer is not None:
            buffer.connect_ranks([peer], activate=False)
        sync(store, args, "prepared-reconnect-done", args.rank, ACTIVE_RANKS)
        check_mask(buffer, expected, "P1_PREPARED_RECONNECT_STILL_MASKED")
        print(f"P1_PREPARED_DISCONNECT_RECONNECT_PASS rank={args.rank}", flush=True)

        for peer in ACTIVE_RANKS:
            if peer != args.rank:
                buffer.update_mask_buffer(peer, mask=False)
        sync(store, args, "all-committed", args.rank, ACTIVE_RANKS)
        check_mask(buffer, [0, 0, 0, 1], "P1_EXPLICIT_COMMIT_MASK")

        sync(store, args, "three-rank-start", args.rank, ACTIVE_RANKS)
        run_round(buffer, args.rank, 3, "three-rank")
        sync(store, args, "three-rank-done", args.rank, ACTIVE_RANKS)
        print(f"P1_THREE_RANK_NUMERIC_PASS rank={args.rank}", flush=True)

        sync(store, args, "missing-barrier-start", args.rank, ACTIVE_RANKS)
        if args.rank == 2:
            wait(store, args, [key(args, "survivor-returned", r) for r in OLD_RANKS])
            print("P1_BARRIER_PEER_2_DID_NOT_ENTER=1", flush=True)
        else:
            start_ns = time.monotonic_ns()
            buffer.barrier()
            torch.cuda.synchronize()
            elapsed_ms = (time.monotonic_ns() - start_ns) / 1e6
            assert (
                elapsed_ms >= args.timeout_ms * 0.5
            ), f"barrier returned before timeout: {elapsed_ms:.1f}ms"
            check_mask(buffer, [0, 0, 1, 1], "P1_BARRIER_TIMEOUT_MASKED_MISSING_RANK")
            store.set(key(args, "survivor-returned", args.rank), b"returned")
            wait(store, args, [key(args, "survivor-returned", r) for r in OLD_RANKS])
            print(
                f"P1_BARRIER_TIMEOUT_SURVIVOR_RETURNED rank={args.rank} elapsed_ms={elapsed_ms:.1f}",
                flush=True,
            )
        sync(store, args, "done", args.rank, ACTIVE_RANKS)
        print(f"P1_CONNECTION_CONTRACT_PASS rank={args.rank}", flush=True)
    finally:
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)
        if thread is None or not thread.is_alive():
            buffer.destroy()


if __name__ == "__main__":
    main()
