#!/usr/bin/env python3
"""Exercise concurrent LOCAL_DISK owner reads (MC_OFFLOAD_OWNER_READ_CONCURRENCY).

Roles:
  owner  contributes a memory segment with SSD offload and stays alive
  write  puts deterministic values until owners evict them to disk
  list   writes the current disk-only keys to a file
  read   reads disk-only keys through one batch API, verifies every byte,
         and reports latency; supports several threads, repeated rounds,
         a dummy (IPC) client, and an expected-failure mode for fault tests

Orchestrated by run_offload_owner_parallel_read.sh.
"""

import argparse
import ctypes
import hashlib
import os
import random
import signal
import statistics
import sys
import threading
import time

from mooncake.store import MooncakeDistributedStore

MB = 1024 * 1024
KEY_PREFIX = "ownerpar"


def payload(key, size):
    return random.Random(hashlib.sha256(key.encode()).digest()).randbytes(size)


def keys_for(num_keys):
    return [f"{KEY_PREFIX}_{i}" for i in range(num_keys)]


def setup(hostname, segment_bytes, local_buffer_bytes, ssd_path=None):
    store = MooncakeDistributedStore()
    kwargs = {}
    if ssd_path:
        os.makedirs(ssd_path, exist_ok=True)
        kwargs = {"enable_ssd_offload": True, "ssd_offload_path": ssd_path}
    rc = store.setup(
        hostname,
        os.environ.get("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata"),
        segment_bytes,
        local_buffer_bytes,
        "tcp",
        "",
        os.environ.get("MASTER_SERVER", "127.0.0.1:50051"),
        **kwargs,
    )
    if rc:
        raise RuntimeError(f"setup failed for {hostname}: rc={rc}")
    return store


def run_owner(args):
    # Keep the store referenced: if it is collected the client stops pinging,
    # the master expires it and unmounts the segment.
    store = setup(args.hostname, args.segment_mb * MB, 16 * MB, args.ssd_path)
    print(f"OWNER_READY {args.hostname} ssd={args.ssd_path}", flush=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    while store is not None:
        time.sleep(3600)


def run_write(args):
    store = setup(args.hostname, 0, args.local_buffer_mb * MB)
    size = args.value_mb * MB
    failed = 0
    for i, key in enumerate(keys_for(args.num_keys)):
        for _ in range(args.put_retries):
            if store.put(key, payload(key, size)) == 0:
                break
            time.sleep(0.2)
        else:
            failed += 1
            # Nothing ever fit: the owners' segments are not usable, so
            # retrying the remaining keys only burns time.
            if failed == i + 1 and failed >= 3:
                sys.exit(f"WRITE_ABORT first {failed} keys all failed")
        if args.put_interval:
            time.sleep(args.put_interval)
        if (i + 1) % 100 == 0:
            print(f"put {i + 1}/{args.num_keys}", flush=True)
    print(f"WRITE_DONE keys={args.num_keys} failed={failed}", flush=True)
    time.sleep(args.settle_seconds)


def disk_only_keys(store, keys):
    descs = store.batch_get_replica_desc(keys)
    picked = []
    for key in keys:
        replicas = descs.get(key) or []
        has_memory = any(r.is_memory_replica() for r in replicas)
        has_local_disk = any(r.is_local_disk_replica() for r in replicas)
        if has_local_disk and not has_memory:
            picked.append(key)
    return picked


def run_list(args):
    store = setup(args.hostname, 0, args.local_buffer_mb * MB)
    keys = disk_only_keys(store, keys_for(args.num_keys))
    with open(args.keys_file, "w") as f:
        f.write("\n".join(keys))
    print(f"disk_only_keys={len(keys)} of {args.num_keys}", flush=True)


def open_reader(args, buffer_bytes):
    """Returns (store, base address of a registered read buffer)."""
    if args.dummy:
        store = MooncakeDistributedStore()
        rc = store.setup_dummy(
            buffer_bytes + 64 * MB, args.local_buffer_mb * MB, args.dummy
        )
        if rc:
            raise RuntimeError(f"setup_dummy failed for {args.dummy}: rc={rc}")
        base = store.alloc_from_mem_pool(buffer_bytes)
        if not base:
            raise RuntimeError("alloc_from_mem_pool failed")
        keepalive = None
    else:
        store = setup(args.hostname, 0, args.local_buffer_mb * MB)
        keepalive = (ctypes.c_ubyte * buffer_bytes)()
        base = ctypes.addressof(keepalive)
    if store.register_buffer(base, buffer_bytes) != 0:
        raise RuntimeError("register_buffer failed")
    return store, base, keepalive


def read_batch(store, api, batch, base, size):
    """Reads one batch; returns per-key bytes, or None where the read failed."""
    if api == "buffer":
        handles = store.batch_get_buffer(batch)
        return [
            bytes(h) if h is not None and h.size() == size else None
            for h in handles
        ]
    ptrs = [base + j * size for j in range(len(batch))]
    if api == "into":
        results = store.batch_get_into(batch, ptrs, [size] * len(batch))
    else:
        # Split every value over two buffers to exercise the scatter path.
        half = size // 2
        results = store.batch_get_into_multi_buffers(
            batch,
            [[p, p + half] for p in ptrs],
            [[half, size - half]] * len(batch),
        )
    return [
        ctypes.string_at(ptrs[j], size) if n == size else None
        for j, n in enumerate(results)
    ]


def run_read(args):
    size = args.value_mb * MB
    if args.dummy and args.api == "buffer":
        sys.exit("FAIL batch_get_buffer does not reach the owner loop via dummy")
    store, base, keepalive = open_reader(args, args.threads * args.batch * size)
    if args.keys_file:
        with open(args.keys_file) as f:
            keys = [k for k in f.read().split("\n") if k]
    else:
        keys = disk_only_keys(store, keys_for(args.num_keys))
    print(f"disk_only_keys={len(keys)} of {args.num_keys}", flush=True)
    if len(keys) < args.batch:
        print("FAIL not enough disk-only keys; raise --num-keys or settle time")
        sys.exit(2)
    expected = {key: payload(key, size) for key in keys}

    batches = [keys[i : i + args.batch] for i in range(0, len(keys), args.batch)]
    batches = batches[: args.max_batches] * args.rounds
    lock = threading.Lock()
    latencies, failed_keys = [], set()
    totals = {"errors": 0, "mismatches": 0, "bytes": 0, "crashed": 0}

    def worker(t):
        my_base = base + t * args.batch * size
        try:
            for batch in batches[t :: args.threads]:
                start = time.perf_counter()
                data = read_batch(store, args.api, batch, my_base, size)
                elapsed = time.perf_counter() - start
                errors = [k for k, d in zip(batch, data) if d is None]
                mismatches = sum(
                    1 for k, d in zip(batch, data)
                    if d is not None and d != expected[k]
                )
                with lock:
                    latencies.append(elapsed)
                    failed_keys.update(errors)
                    totals["errors"] += len(errors)
                    totals["mismatches"] += mismatches
                    totals["bytes"] += size * (len(batch) - len(errors))
        except Exception as e:
            with lock:
                totals["crashed"] += 1
            print(f"worker {t} crashed: {e!r}", flush=True)

    wall_start = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(t,)) for t in range(args.threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    wall = time.perf_counter() - wall_start
    del keepalive

    if args.failed_keys_file:
        with open(args.failed_keys_file, "w") as f:
            f.write("\n".join(sorted(failed_keys)))
    print(
        f"RESULT api={args.api} dummy={bool(args.dummy)} threads={args.threads} "
        f"concurrency={os.environ.get('MC_OFFLOAD_OWNER_READ_CONCURRENCY', '1')} "
        f"batches={len(latencies)} batch={args.batch} errors={totals['errors']} "
        f"failed_keys={len(failed_keys)} mismatches={totals['mismatches']} "
        f"crashed={totals['crashed']} "
        f"p50_ms={statistics.median(latencies) * 1e3 if latencies else 0:.1f} "
        f"max_ms={max(latencies, default=0) * 1e3:.1f} "
        f"throughput_MBps={totals['bytes'] / MB / wall:.1f}",
        flush=True,
    )
    ok = totals["mismatches"] == 0 and totals["crashed"] == 0
    if args.expect_failures:
        # Some keys must fail (the dead owner's), others must still succeed.
        ok = ok and 0 < len(failed_keys) < len(keys)
    else:
        ok = ok and totals["errors"] == 0
    sys.exit(0 if ok else 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["owner", "write", "list", "read"])
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--segment-mb", type=int, default=256)
    parser.add_argument("--ssd-path")
    parser.add_argument("--local-buffer-mb", type=int, default=128)
    parser.add_argument("--value-mb", type=int, default=4)
    parser.add_argument("--num-keys", type=int, default=600)
    parser.add_argument("--batch", type=int, default=8)
    parser.add_argument("--max-batches", type=int, default=20)
    parser.add_argument("--put-retries", type=int, default=50)
    parser.add_argument("--put-interval", type=float, default=0.0)
    parser.add_argument("--settle-seconds", type=float, default=10.0)
    parser.add_argument("--api", choices=["into", "multi", "buffer"], default="into")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--dummy", help="real client address; read via IPC")
    parser.add_argument("--keys-file")
    parser.add_argument("--failed-keys-file")
    parser.add_argument("--expect-failures", action="store_true")
    args = parser.parse_args()
    roles = {"owner": run_owner, "write": run_write, "list": run_list, "read": run_read}
    roles[args.role](args)


if __name__ == "__main__":
    main()
