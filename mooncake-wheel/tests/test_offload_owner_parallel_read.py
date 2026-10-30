#!/usr/bin/env python3
"""Exercise concurrent LOCAL_DISK owner reads (MC_OFFLOAD_OWNER_READ_CONCURRENCY).

Roles:
  owner  contributes a memory segment with SSD offload and stays alive
  write  puts deterministic values until owners evict them to disk
  read   batch_get_into disk-only keys, verifies bytes, reports latency

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


def run_read(args):
    store = setup(args.hostname, 0, args.local_buffer_mb * MB)
    size = args.value_mb * MB
    keys = disk_only_keys(store, keys_for(args.num_keys))
    concurrency = os.environ.get("MC_OFFLOAD_OWNER_READ_CONCURRENCY", "1")
    print(f"disk_only_keys={len(keys)} of {args.num_keys}", flush=True)
    if len(keys) < args.batch:
        print("FAIL not enough disk-only keys; raise --num-keys or settle time")
        sys.exit(2)

    buffer = (ctypes.c_ubyte * (args.batch * size))()
    base = ctypes.addressof(buffer)
    if store.register_buffer(base, args.batch * size) != 0:
        raise RuntimeError("register_buffer failed")

    latencies, errors, mismatches, total_bytes = [], 0, 0, 0
    batches = [keys[i : i + args.batch] for i in range(0, len(keys), args.batch)]
    for batch in batches[: args.max_batches]:
        ptrs = [base + j * size for j in range(len(batch))]
        start = time.perf_counter()
        results = store.batch_get_into(batch, ptrs, [size] * len(batch))
        latencies.append(time.perf_counter() - start)
        for j, (key, n) in enumerate(zip(batch, results)):
            if n != size:
                errors += 1
                continue
            total_bytes += n
            if ctypes.string_at(ptrs[j], n) != payload(key, size):
                mismatches += 1

    elapsed = sum(latencies)
    print(
        f"RESULT concurrency={concurrency} batches={len(latencies)} "
        f"batch={args.batch} errors={errors} mismatches={mismatches} "
        f"p50_ms={statistics.median(latencies) * 1e3:.1f} "
        f"max_ms={max(latencies) * 1e3:.1f} "
        f"throughput_MBps={total_bytes / MB / elapsed:.1f}",
        flush=True,
    )
    sys.exit(1 if errors or mismatches else 0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["owner", "write", "read"])
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--segment-mb", type=int, default=256)
    parser.add_argument("--ssd-path")
    parser.add_argument("--local-buffer-mb", type=int, default=128)
    parser.add_argument("--value-mb", type=int, default=4)
    parser.add_argument("--num-keys", type=int, default=600)
    parser.add_argument("--batch", type=int, default=32)
    parser.add_argument("--max-batches", type=int, default=20)
    parser.add_argument("--put-retries", type=int, default=50)
    parser.add_argument("--put-interval", type=float, default=0.0)
    parser.add_argument("--settle-seconds", type=float, default=10.0)
    args = parser.parse_args()
    {"owner": run_owner, "write": run_write, "read": run_read}[args.role](args)


if __name__ == "__main__":
    main()
