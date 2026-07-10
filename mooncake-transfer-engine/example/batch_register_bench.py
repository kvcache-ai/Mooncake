#!/usr/bin/env python3
"""
Batch Memory Registration Benchmark for Mooncake EFA Transport.

Tests registering multiple independent memory blocks (simulating
multi-tenant or sharded KV cache pools) and measures registration
time and transfer throughput across blocks.

Usage:
  # Target node (registers memory blocks):
  python batch_register_bench.py --mode target \
      --local_server_name <target_ip>:12345 \
      --num_blocks 40 --block_size_gb 4 --protocol efa

  # Initiator node (pulls data from random blocks):
  python batch_register_bench.py --mode initiator \
      --local_server_name <initiator_ip>:12346 \
      --target_server_name <target_ip>:12345 \
      --num_blocks 40 --block_size_gb 4 --protocol efa

Requires: mooncake Python package (pip install -e mooncake-wheel)
"""

import argparse
import ctypes
import ctypes.util
import json
import os
import random
import signal
import statistics
import sys
import time


def parse_args():
    parser = argparse.ArgumentParser(
        description="Batch Memory Registration Benchmark"
    )
    parser.add_argument(
        "--mode",
        choices=["target", "initiator"],
        required=True,
        help="Run as target (memory holder) or initiator (puller)",
    )
    parser.add_argument(
        "--local_server_name",
        required=True,
        help="Local address, e.g. 172.31.6.162:12345",
    )
    parser.add_argument(
        "--target_server_name",
        default="",
        help="Target address (initiator mode only)",
    )
    parser.add_argument(
        "--metadata_server",
        default="P2PHANDSHAKE",
        help="Metadata server address (default: P2PHANDSHAKE)",
    )
    parser.add_argument(
        "--protocol", default="efa", help="Transport protocol (default: efa)"
    )
    parser.add_argument(
        "--num_blocks",
        type=int,
        default=40,
        help="Number of memory blocks to register (default: 40)",
    )
    parser.add_argument(
        "--block_size_gb",
        type=float,
        default=4.0,
        help="Size of each memory block in GB (default: 4)",
    )
    parser.add_argument(
        "--transfer_size_mb",
        type=float,
        default=368.0,
        help="Transfer size in MB per iteration (default: 368, ~4K tokens GLM-5.1)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=20,
        help="Number of transfer iterations (default: 20)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=5,
        help="Number of warmup iterations (default: 5)",
    )
    parser.add_argument(
        "--use_batch_api",
        action="store_true",
        help="Use batch_register_memory API instead of per-block register_memory",
    )
    return parser.parse_args()


def allocate_block(size_bytes):
    """Allocate a single page-aligned memory block using mmap."""
    libc_name = ctypes.util.find_library("c")
    libc = ctypes.CDLL(libc_name, use_errno=True)

    PROT_READ = 0x1
    PROT_WRITE = 0x2
    MAP_PRIVATE = 0x02
    MAP_ANONYMOUS = 0x20
    MAP_HUGETLB = 0x40000
    MAP_FAILED = ctypes.c_void_p(-1).value

    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [
        ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
        ctypes.c_int, ctypes.c_int, ctypes.c_long,
    ]

    flags = MAP_PRIVATE | MAP_ANONYMOUS

    # Try hugepages first
    ptr = libc.mmap(None, size_bytes, PROT_READ | PROT_WRITE,
                    flags | MAP_HUGETLB, -1, 0)
    if ptr and ptr != MAP_FAILED:
        return ptr, True

    # Fall back to regular pages
    ptr = libc.mmap(None, size_bytes, PROT_READ | PROT_WRITE, flags, -1, 0)
    if not ptr or ptr == MAP_FAILED:
        raise RuntimeError(
            f"mmap failed for {size_bytes} bytes: errno={ctypes.get_errno()}"
        )
    return ptr, False


def run_target(args):
    """Run as target: allocate blocks, register, and wait."""
    from mooncake.engine import TransferEngine

    block_bytes = int(args.block_size_gb * 1024 * 1024 * 1024)
    total_gb = args.num_blocks * args.block_size_gb

    print(f"=== Target Node ===")
    print(f"Blocks: {args.num_blocks} x {args.block_size_gb} GB = {total_gb} GB total")
    print(f"Protocol: {args.protocol}")
    print(f"Registration API: {'batch' if args.use_batch_api else 'per-block'}")

    engine = TransferEngine()
    ret = engine.initialize(
        args.local_server_name, args.metadata_server, args.protocol, ""
    )
    if ret != 0:
        raise RuntimeError(f"Engine initialization failed: {ret}")

    if args.metadata_server == "P2PHANDSHAKE":
        host = args.local_server_name.rpartition(":")[0]
        rpc_port = engine.get_rpc_port()
        actual_name = f"{host}:{rpc_port}"
        print(f"Actual server name: {actual_name}")

    # Allocate blocks
    print(f"\nAllocating {args.num_blocks} blocks of {args.block_size_gb} GB...")
    blocks = []  # (addr, size, is_hugepage)
    for i in range(args.num_blocks):
        try:
            ptr, hp = allocate_block(block_bytes)
            blocks.append((ptr, block_bytes, hp))
            if (i + 1) % 10 == 0 or i == 0:
                page_type = "hugepage" if hp else "4KB"
                print(f"  Block {i+1}/{args.num_blocks}: 0x{ptr:x} ({page_type})")
        except RuntimeError as e:
            print(f"  Block {i+1} allocation FAILED: {e}")
            break

    if not blocks:
        raise RuntimeError("No blocks allocated")

    hp_count = sum(1 for _, _, hp in blocks if hp)
    reg_count = sum(1 for _, _, hp in blocks if not hp)
    print(f"  Allocated {len(blocks)} blocks: {hp_count} hugepage, {reg_count} regular")

    # Register
    addrs = [b[0] for b in blocks]
    sizes = [b[1] for b in blocks]

    print(f"\nRegistering {len(blocks)} blocks...")
    t0 = time.time()

    if args.use_batch_api:
        ret = engine.batch_register_memory(addrs, sizes)
        if ret != 0:
            print(f"  batch_register_memory FAILED: {ret}")
            print(f"  Registration took {time.time() - t0:.1f}s before failure")
            return
    else:
        for i, (addr, size, _) in enumerate(blocks):
            ret = engine.register_memory(addr, size)
            if ret != 0:
                print(f"  register_memory FAILED at block {i}: {ret}")
                print(f"  Registration took {time.time() - t0:.1f}s before failure")
                return

    reg_time = time.time() - t0
    print(f"  Registration OK: {reg_time:.1f}s for {len(blocks)} blocks")
    print(f"  Per-block: {reg_time / len(blocks) * 1000:.0f}ms")

    # Publish block info
    info = {
        "blocks": [{"addr": b[0], "size": b[1]} for b in blocks],
        "num_blocks": len(blocks),
    }
    print(f"\nTarget ready with {len(blocks)} blocks.")
    print(f"TARGET_INFO:{json.dumps(info)}")
    print("Waiting for initiator (Ctrl+C to stop)...")

    try:
        signal.pause()
    except KeyboardInterrupt:
        print("\nShutting down target.")


def run_initiator(args):
    """Run as initiator: pull data from random blocks."""
    from mooncake.engine import TransferEngine

    block_bytes = int(args.block_size_gb * 1024 * 1024 * 1024)
    transfer_bytes = int(args.transfer_size_mb * 1024 * 1024)

    print(f"=== Initiator Node ===")
    print(f"Target: {args.target_server_name}")
    print(f"Blocks: {args.num_blocks} x {args.block_size_gb} GB")
    print(f"Transfer size: {args.transfer_size_mb} MB")
    print(f"Protocol: {args.protocol}")

    if not args.target_server_name:
        raise RuntimeError("--target_server_name required in initiator mode")

    engine = TransferEngine()
    ret = engine.initialize(
        args.local_server_name, args.metadata_server, args.protocol, ""
    )
    if ret != 0:
        raise RuntimeError(f"Engine initialization failed: {ret}")

    if args.metadata_server == "P2PHANDSHAKE":
        host = args.local_server_name.rpartition(":")[0]
        rpc_port = engine.get_rpc_port()
        actual_name = f"{host}:{rpc_port}"
        print(f"Actual server name: {actual_name}")

    # Allocate local receive buffer
    recv_bytes = transfer_bytes
    print(f"\nAllocating {recv_bytes / 1e6:.0f} MB receive buffer...")
    recv_addr, hp = allocate_block(recv_bytes)
    print(f"  0x{recv_addr:x} ({'hugepage' if hp else '4KB'})")
    ret = engine.register_memory(recv_addr, recv_bytes)
    if ret != 0:
        raise RuntimeError(f"Local memory registration failed: {ret}")

    # Get target's first buffer address
    print(f"Connecting to target {args.target_server_name}...")
    remote_base = engine.get_first_buffer_address(args.target_server_name)
    if remote_base == 0:
        raise RuntimeError("Cannot get target buffer address")
    print(f"  Remote first buffer at 0x{remote_base:x}")

    # Connection warmup
    print("Warming up connection...")
    for _ in range(3):
        engine.transfer_sync_read(
            args.target_server_name, recv_addr, remote_base, transfer_bytes
        )
    print("  Connection ready.")

    # Note: blocks are independently mmap'd on the target, so they are NOT
    # contiguous. We can only access the first block via get_first_buffer_address.
    # The primary goal is to validate that registration of N separate blocks works
    # and that data can be transferred from a registered block.
    print(f"\nBenchmarking transfers from first registered block...")
    print(f"  Each transfer: {args.transfer_size_mb} MB")

    # Warmup
    print(f"  Warming up ({args.warmup} iterations)...")
    for w in range(args.warmup):
        ret = engine.transfer_sync_read(
            args.target_server_name, recv_addr, remote_base, transfer_bytes
        )
        if ret != 0:
            print(f"  WARNING: warmup failed iter {w}: {ret}")

    # Benchmark
    latencies = []
    errors = 0
    for i in range(args.iterations):
        t0 = time.perf_counter()
        ret = engine.transfer_sync_read(
            args.target_server_name, recv_addr, remote_base, transfer_bytes
        )
        elapsed = time.perf_counter() - t0

        if ret != 0:
            errors += 1
            if errors <= 3:
                print(f"  ERROR: transfer failed iter {i}: {ret}")
            continue

        latencies.append(elapsed * 1000)

    if not latencies:
        print(f"  ALL FAILED ({errors} errors)")
        return

    latencies.sort()
    avg_ms = statistics.mean(latencies)
    p50_ms = latencies[len(latencies) // 2]
    p99_ms = latencies[int(len(latencies) * 0.99)]
    throughput_gbs = (transfer_bytes / 1e9) / (p50_ms / 1000)

    print(f"\n{'='*60}")
    print(f"  Registered blocks: {args.num_blocks} x {args.block_size_gb}GB")
    print(f"  Transfer: {args.transfer_size_mb} MB")
    print(f"  Iterations: {len(latencies)} (errors: {errors})")
    print(f"  Avg latency:  {avg_ms:.2f} ms")
    print(f"  p50 latency:  {p50_ms:.2f} ms")
    print(f"  p99 latency:  {p99_ms:.2f} ms")
    print(f"  Throughput:   {throughput_gbs:.2f} GB/s")
    print(f"{'='*60}")

    results = {
        "num_blocks": args.num_blocks,
        "block_size_gb": args.block_size_gb,
        "transfer_size_mb": args.transfer_size_mb,
        "iterations": len(latencies),
        "errors": errors,
        "avg_latency_ms": round(avg_ms, 3),
        "p50_latency_ms": round(p50_ms, 3),
        "p99_latency_ms": round(p99_ms, 3),
        "throughput_gbs": round(throughput_gbs, 3),
    }

    out_file = f"batch_bench_{args.num_blocks}x{args.block_size_gb}gb.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {out_file}")


def main():
    args = parse_args()
    if args.mode == "target":
        run_target(args)
    else:
        run_initiator(args)


if __name__ == "__main__":
    main()
