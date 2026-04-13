#!/usr/bin/env python3
"""
Cross-node Get benchmark for Mooncake Store.

Measures regular Get latency/throughput across two nodes over TCP and RDMA,
with small / medium / large object tiers and varying value sizes.

Usage (requires a running mooncake_master on the master node):

  # Node 0 (provider — hosts the data):
  python cross_node_get_benchmark.py --role provider \
      --local-hostname 192.168.22.70 --protocol rdma

  # Node 1 (consumer — fetches the data):
  python cross_node_get_benchmark.py --role consumer \
      --local-hostname 192.168.22.72 --protocol rdma

Or use the companion run_cross_node_bench.sh to automate both sides.
"""

import argparse
import logging
import os
import statistics
import sys
import time

import numpy as np

os.environ["MC_STORE_MEMCPY"] = "0"

from mooncake.store import MooncakeDistributedStore, ReplicateConfig  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cross_node_bench")

# ---------------------------------------------------------------------------
# Benchmark configuration
# ---------------------------------------------------------------------------

TIERS = [
    {
        "label": "SMALL",
        "sizes": [
            64 * 1024,          # 64 KB
            256 * 1024,         # 256 KB
            1 * 1024 * 1024,    # 1 MB
            4 * 1024 * 1024,    # 4 MB
        ],
        "warmup": 3,
        "iters": 10,
    },
    {
        "label": "MEDIUM",
        "sizes": [
            16 * 1024 * 1024,   # 16 MB
            64 * 1024 * 1024,   # 64 MB
            128 * 1024 * 1024,  # 128 MB
        ],
        "warmup": 2,
        "iters": 5,
    },
    {
        "label": "LARGE",
        "sizes": [
            256 * 1024 * 1024,           # 256 MB
            512 * 1024 * 1024,           # 512 MB
            1 * 1024 * 1024 * 1024,      # 1 GB
        ],
        "warmup": 1,
        "iters": 3,
    },
]


def fmt_bytes(n: int) -> str:
    if n >= 1024 * 1024 * 1024:
        return f"{n / (1024**3):.0f}GB"
    if n >= 1024 * 1024:
        return f"{n / (1024**2):.0f}MB"
    return f"{n / 1024:.0f}KB"


def median(v):
    s = sorted(v)
    n = len(s)
    if n == 0:
        return 0.0
    return (s[(n - 1) // 2] + s[n // 2]) / 2.0


# ---------------------------------------------------------------------------
# Provider: puts objects and waits for consumer to finish
# ---------------------------------------------------------------------------

def run_provider(args):
    store = MooncakeDistributedStore()
    seg_size = args.global_segment_size * 1024 * 1024
    buf_size = args.local_buffer_size * 1024 * 1024

    rc = store.setup(
        args.local_hostname, args.metadata_server,
        seg_size, buf_size,
        args.protocol, args.device_name, args.master_server,
    )
    if rc:
        log.error(f"store.setup failed: {rc}")
        sys.exit(1)

    # Allocate a big buffer for Put
    max_size = max(s for t in TIERS for s in t["sizes"])
    buf = np.zeros(max_size, dtype=np.uint8)
    buf_ptr = buf.ctypes.data
    rc = store.register_buffer(buf_ptr, max_size)
    if rc:
        log.error(f"register_buffer failed: {rc}")
        sys.exit(1)

    log.info("Provider ready, putting objects ...")

    for tier in TIERS:
        for obj_size in tier["sizes"]:
            key = f"bench_{obj_size}"
            # Fill with deterministic pattern
            pattern_byte = (obj_size // 1024) & 0xFF
            buf[:obj_size] = pattern_byte

            rc_list = store.batch_put_from([key], [buf_ptr], [obj_size])
            if rc_list[0] != 0:
                log.error(f"Put {key} failed: {rc_list[0]}")
                sys.exit(1)
            log.info(f"  put {key} ({fmt_bytes(obj_size)}) ok")

    log.info("All objects stored. Waiting for consumer (Ctrl-C to stop) ...")

    # Write a sentinel key so consumer knows provider is ready
    sentinel = np.zeros(8, dtype=np.uint8)
    sentinel_ptr = sentinel.ctypes.data
    # sentinel is tiny, use the already-registered buffer region
    buf[:8] = 0x42
    store.batch_put_from(["__bench_ready__"], [buf_ptr], [8])

    try:
        while True:
            # Check if consumer wrote a "done" sentinel
            done_rc = store.is_exist("__bench_done__")
            if done_rc == 1:
                log.info("Consumer signalled done. Cleaning up.")
                break
            time.sleep(2)
    except KeyboardInterrupt:
        log.info("Interrupted.")

    # Cleanup
    for tier in TIERS:
        for obj_size in tier["sizes"]:
            store.remove(f"bench_{obj_size}")
    store.remove("__bench_ready__")
    store.remove("__bench_done__")

    store.unregister_buffer(buf_ptr)
    store.close()
    log.info("Provider done.")


# ---------------------------------------------------------------------------
# Consumer: waits for provider, then benchmarks Get
# ---------------------------------------------------------------------------

def run_consumer(args):
    store = MooncakeDistributedStore()
    buf_size = args.local_buffer_size * 1024 * 1024

    rc = store.setup(
        args.local_hostname, args.metadata_server,
        0, buf_size,  # consumer doesn't need global segment
        args.protocol, args.device_name, args.master_server,
    )
    if rc:
        log.error(f"store.setup failed: {rc}")
        sys.exit(1)

    # Allocate receive buffer
    max_size = max(s for t in TIERS for s in t["sizes"])
    buf = np.zeros(max_size, dtype=np.uint8)
    buf_ptr = buf.ctypes.data
    rc = store.register_buffer(buf_ptr, max_size)
    if rc:
        log.error(f"register_buffer failed: {rc}")
        sys.exit(1)

    # Wait for provider
    log.info("Consumer waiting for provider ...")
    for attempt in range(120):
        if store.is_exist("__bench_ready__") == 1:
            break
        time.sleep(1)
    else:
        log.error("Timed out waiting for provider")
        sys.exit(1)
    log.info("Provider ready. Starting benchmark.")

    # -----------------------------------------------------------------------
    print()
    print(f"{'='*80}")
    print(f"  Cross-Node Get Benchmark  |  protocol={args.protocol}")
    print(f"{'='*80}")

    for tier in TIERS:
        print(f"\n--- {tier['label']} tier ---")
        print(f"  {'size':>8s}  {'median_us':>10s}  {'mean_us':>10s}  "
              f"{'min_us':>10s}  {'max_us':>10s}  {'bw_MB/s':>10s}")

        for obj_size in tier["sizes"]:
            key = f"bench_{obj_size}"
            warmup = tier["warmup"]
            iters = tier["iters"]

            latencies_us = []
            for i in range(warmup + iters):
                t0 = time.perf_counter()
                rc_list = store.batch_get_into([key], [buf_ptr], [obj_size])
                t1 = time.perf_counter()

                if rc_list[0] < 0:
                    log.error(f"Get {key} iter {i} failed: {rc_list[0]}")
                    continue

                if i >= warmup:
                    latencies_us.append((t1 - t0) * 1e6)

            if not latencies_us:
                print(f"  {fmt_bytes(obj_size):>8s}  {'FAILED':>10s}")
                continue

            med = median(latencies_us)
            avg = statistics.mean(latencies_us)
            lo = min(latencies_us)
            hi = max(latencies_us)
            bw = obj_size / (med / 1e6) / (1024 * 1024) if med > 0 else 0

            print(f"  {fmt_bytes(obj_size):>8s}  {med:10.0f}  {avg:10.0f}  "
                  f"{lo:10.0f}  {hi:10.0f}  {bw:10.1f}")

    print(f"\n{'='*80}")
    print()

    # Signal provider we are done
    buf[:8] = 0x99
    store.batch_put_from(["__bench_done__"], [buf_ptr], [8])

    store.unregister_buffer(buf_ptr)
    store.close()
    log.info("Consumer done.")


# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Cross-node Mooncake Get benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--role", choices=["provider", "consumer"], required=True)
    parser.add_argument("--protocol", default="rdma")
    parser.add_argument("--device-name", default="", help="RDMA device (empty=auto)")
    parser.add_argument("--local-hostname", required=True)
    parser.add_argument("--metadata-server", default="http://192.168.22.70:8080/metadata")
    parser.add_argument("--master-server", default="192.168.22.70:50051")
    parser.add_argument("--global-segment-size", type=int, default=4096,
                        help="Provider segment size in MB")
    parser.add_argument("--local-buffer-size", type=int, default=2048,
                        help="Local buffer size in MB")
    args = parser.parse_args()
    if args.role == "provider":
        run_provider(args)
    else:
        run_consumer(args)


if __name__ == "__main__":
    main()
