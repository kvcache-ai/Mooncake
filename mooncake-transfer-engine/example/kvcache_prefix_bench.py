#!/usr/bin/env python3
"""
KV Cache Prefix Transfer Benchmark for Mooncake EFA Transport.

Simulates cross-node KV cache transfer for prefix cache hits in LLM inference.
Tests how registered memory pool size affects transfer latency/throughput
under the per-NIC partition auto-split strategy.

Usage:
  # Target node (holds KV cache pool):
  python kvcache_prefix_bench.py --mode target \
      --local_server_name <target_ip>:12345 \
      --pool_size_gb 100 --protocol efa

  # Initiator node (pulls prefix KV cache):
  python kvcache_prefix_bench.py --mode initiator \
      --local_server_name <initiator_ip>:12346 \
      --target_server_name <target_ip>:12345 \
      --pool_size_gb 100 --protocol efa \
      --prefix_tokens 4096,8192,16384,32768

Requires: mooncake Python package (pip install -e mooncake-wheel)
Branch: feat/efa-auto-split-mr (per-NIC partition auto-split)
"""

import argparse
import ctypes
import ctypes.util
import json
import os
import signal
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def parse_args():
    parser = argparse.ArgumentParser(
        description="KV Cache Prefix Transfer Benchmark"
    )
    parser.add_argument(
        "--mode",
        choices=["target", "initiator"],
        required=True,
        help="Run as target (KV cache holder) or initiator (puller)",
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
        "--pool_size_gb",
        type=float,
        default=10.0,
        help="KV cache pool size in GB to register (default: 10)",
    )
    parser.add_argument(
        "--prefix_tokens",
        default="4096,8192,16384,32768",
        help="Comma-separated list of prefix token counts to test",
    )
    parser.add_argument(
        "--kv_bytes_per_token",
        type=int,
        default=89856,
        help="KV cache bytes per token. "
        "Default: 89856 for GLM-5.1 (754B MoE, MLA attention): "
        "(kv_lora_rank=512 + qk_rope_head_dim=64) * 2 bytes * 78 layers. "
        "For GLM-4-9B (standard MHA): use 40960",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=50,
        help="Number of transfer iterations per test (default: 50)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=5,
        help="Number of warmup iterations (default: 5)",
    )
    parser.add_argument(
        "--use_gpu",
        action="store_true",
        help="Use GPU memory instead of CPU memory",
    )
    parser.add_argument(
        "--gpu_id", type=int, default=0, help="GPU device ID (default: 0)"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Number of threads for concurrent transfer (default: 1). "
        "Each thread transfers a chunk of the prefix in parallel.",
    )
    return parser.parse_args()


def allocate_cpu_memory(size_bytes):
    """Allocate page-aligned CPU memory using mmap."""
    libc_name = ctypes.util.find_library("c")
    libc = ctypes.CDLL(libc_name, use_errno=True)

    # mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0)
    PROT_READ = 0x1
    PROT_WRITE = 0x2
    MAP_PRIVATE = 0x02
    MAP_ANONYMOUS = 0x20
    MAP_HUGETLB = 0x40000
    MAP_FAILED = ctypes.c_void_p(-1).value

    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_long,
    ]

    flags = MAP_PRIVATE | MAP_ANONYMOUS

    # Try hugepages first (recommended for large EFA registrations)
    ptr = libc.mmap(None, size_bytes, PROT_READ | PROT_WRITE,
                    flags | MAP_HUGETLB, -1, 0)
    if ptr and ptr != MAP_FAILED:
        print(f"  Allocated {size_bytes / 1e9:.1f} GB with 2MB hugepages")
        return ptr, size_bytes, True

    # Fall back to regular pages
    ptr = libc.mmap(None, size_bytes, PROT_READ | PROT_WRITE, flags, -1, 0)
    if not ptr or ptr == MAP_FAILED:
        raise RuntimeError(
            f"mmap failed for {size_bytes} bytes: "
            f"errno={ctypes.get_errno()}"
        )
    print(
        f"  Allocated {size_bytes / 1e9:.1f} GB with 4KB pages "
        f"(hugepages unavailable — configure vm.nr_hugepages for better EFA performance)"
    )
    return ptr, size_bytes, False


def allocate_gpu_memory(size_bytes, gpu_id):
    """Allocate GPU memory using PyTorch."""
    import torch

    torch.cuda.set_device(gpu_id)
    # Allocate as uint8 tensor
    tensor = torch.zeros(size_bytes, dtype=torch.uint8, device=f"cuda:{gpu_id}")
    ptr = tensor.data_ptr()
    print(f"  Allocated {size_bytes / 1e9:.1f} GB on GPU {gpu_id}")
    return ptr, tensor  # keep tensor alive


def run_target(args):
    """Run as target node: allocate KV cache pool and wait."""
    from mooncake.engine import TransferEngine

    print(f"=== Target Node ===")
    print(f"Pool size: {args.pool_size_gb} GB")
    print(f"Protocol: {args.protocol}")

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

    pool_bytes = int(args.pool_size_gb * 1024 * 1024 * 1024)

    print(f"Allocating {args.pool_size_gb} GB KV cache pool...")
    if args.use_gpu:
        pool_addr, _tensor = allocate_gpu_memory(pool_bytes, args.gpu_id)
    else:
        pool_addr, _, _ = allocate_cpu_memory(pool_bytes)

    print(f"Registering memory with transfer engine...")
    t0 = time.time()
    ret = engine.register_memory(pool_addr, pool_bytes)
    reg_time = time.time() - t0
    if ret != 0:
        raise RuntimeError(f"Memory registration failed: {ret}")
    print(f"  Registration took {reg_time:.1f}s")

    print(f"\nTarget ready. Pool addr: 0x{pool_addr:x}, size: {pool_bytes}")
    print("Waiting for initiator (Ctrl+C to stop)...")

    # Write pool info for initiator to read
    info = {"pool_addr": pool_addr, "pool_bytes": pool_bytes}
    print(f"TARGET_INFO:{json.dumps(info)}")

    try:
        signal.pause()
    except KeyboardInterrupt:
        print("\nShutting down target.")


def run_initiator(args):
    """Run as initiator node: pull prefix KV cache and measure performance."""
    from mooncake.engine import TransferEngine

    print(f"=== Initiator Node ===")
    print(f"Target: {args.target_server_name}")
    print(f"Pool size: {args.pool_size_gb} GB")
    print(f"Protocol: {args.protocol}")
    print(f"KV bytes/token: {args.kv_bytes_per_token}")
    print(f"Threads: {args.threads}")

    if not args.target_server_name:
        raise RuntimeError("--target_server_name required in initiator mode")

    prefix_tokens_list = [int(x) for x in args.prefix_tokens.split(",")]
    transfer_sizes = [
        tokens * args.kv_bytes_per_token for tokens in prefix_tokens_list
    ]

    print(f"\nTest matrix:")
    for tokens, size in zip(prefix_tokens_list, transfer_sizes):
        print(f"  {tokens:>6} tokens -> {size / 1e6:.1f} MB transfer")

    # Initialize engine
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

    # Allocate local receive buffer (large enough for the biggest transfer)
    max_transfer = max(transfer_sizes)
    recv_bytes = max_transfer
    print(f"\nAllocating {recv_bytes / 1e6:.1f} MB local receive buffer...")
    if args.use_gpu:
        recv_addr, _tensor = allocate_gpu_memory(recv_bytes, args.gpu_id)
    else:
        recv_addr, _, _ = allocate_cpu_memory(recv_bytes)

    ret = engine.register_memory(recv_addr, recv_bytes)
    if ret != 0:
        raise RuntimeError(f"Local memory registration failed: {ret}")

    # Get target's buffer address
    print(f"Connecting to target {args.target_server_name}...")
    remote_addr = engine.get_first_buffer_address(args.target_server_name)
    if remote_addr == 0:
        raise RuntimeError(
            "Cannot get target buffer address. "
            "Is the target running and registered?"
        )
    print(f"  Remote buffer at 0x{remote_addr:x}")

    # Connection warmup: a small transfer to establish the EFA connection
    # (openSegment, endpoint creation, etc.) so it doesn't skew the first
    # prefix size's measurements.
    print("Warming up connection...")
    warmup_size = min(transfer_sizes[0], recv_bytes)
    for _ in range(3):
        engine.transfer_sync_read(
            args.target_server_name, recv_addr, remote_addr, warmup_size
        )
    print("  Connection ready.")

    num_threads = args.threads

    def do_transfer(local_addr, remote_addr_with_offset, size):
        """Single transfer_sync_read call, suitable for thread pool."""
        return engine.transfer_sync_read(
            args.target_server_name, local_addr,
            remote_addr_with_offset, size
        )

    def threaded_transfer(local_base, remote_base, total_size, pool):
        """Split transfer across threads; return max of 0 (ok) or error code."""
        if num_threads <= 1:
            return engine.transfer_sync_read(
                args.target_server_name, local_base, remote_base, total_size
            )
        chunk = total_size // num_threads
        # Align chunk to 4KB
        chunk = chunk & ~0xFFF
        futures = []
        for t in range(num_threads):
            off = t * chunk
            sz = chunk if t < num_threads - 1 else (total_size - off)
            futures.append(
                pool.submit(do_transfer, local_base + off,
                            remote_base + off, sz)
            )
        return max(f.result() for f in futures)

    # Run benchmarks
    print(f"\n{'='*72}")
    print(
        f"{'Prefix':>8} {'Size':>10} {'Latency(ms)':>12} "
        f"{'p50(ms)':>10} {'p99(ms)':>10} {'Tput(GB/s)':>12}"
    )
    print(f"{'='*72}")

    results = []
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        for tokens, transfer_size in zip(prefix_tokens_list, transfer_sizes):
            if transfer_size > recv_bytes:
                print(f"  SKIP {tokens} tokens: transfer {transfer_size} > recv buffer")
                continue

            # Warmup: use the same offset pattern as the benchmark to pre-warm
            # remote memory pages and DMA paths at each offset.
            pool_bytes = int(args.pool_size_gb * 1024 * 1024 * 1024)
            max_offset = pool_bytes - transfer_size
            for w in range(args.warmup):
                for i in range(args.iterations):
                    if max_offset > 0:
                        offset = ((i * transfer_size) % max_offset) & ~0xFFF
                    else:
                        offset = 0
                    ret = threaded_transfer(
                        recv_addr, remote_addr + offset, transfer_size, pool
                    )
                    if ret != 0:
                        print(f"  WARNING: warmup transfer failed: {ret}")
                        break

            # Benchmark
            latencies = []
            errors = 0
            for i in range(args.iterations):
                # Use different offset within remote pool for each iteration
                # to simulate accessing different prefix locations
                if max_offset > 0:
                    # Align to 4KB boundary
                    offset = ((i * transfer_size) % max_offset) & ~0xFFF
                else:
                    offset = 0

                t0 = time.perf_counter()
                ret = threaded_transfer(
                    recv_addr, remote_addr + offset, transfer_size, pool
                )
                elapsed = time.perf_counter() - t0

                if ret != 0:
                    errors += 1
                    if errors <= 3:
                        print(f"  ERROR: transfer failed at iter {i}: {ret}")
                    continue

                latencies.append(elapsed * 1000)  # ms

            if not latencies:
                print(f"  {tokens:>6}k  ALL FAILED ({errors} errors)")
                continue

            latencies.sort()
            avg_ms = statistics.mean(latencies)
            p50_ms = latencies[len(latencies) // 2]
            p99_ms = latencies[int(len(latencies) * 0.99)]
            throughput_gbs = (transfer_size / 1e9) / (p50_ms / 1000)

            print(
                f"  {tokens:>6} {transfer_size/1e6:>8.1f}MB "
                f"{avg_ms:>11.2f} {p50_ms:>10.2f} {p99_ms:>10.2f} "
                f"{throughput_gbs:>11.2f}"
            )

            results.append(
                {
                    "prefix_tokens": tokens,
                    "transfer_bytes": transfer_size,
                    "transfer_mb": transfer_size / 1e6,
                    "pool_size_gb": args.pool_size_gb,
                    "iterations": len(latencies),
                    "errors": errors,
                    "avg_latency_ms": round(avg_ms, 3),
                    "p50_latency_ms": round(p50_ms, 3),
                    "p99_latency_ms": round(p99_ms, 3),
                    "throughput_gbs": round(throughput_gbs, 3),
                    "threads": num_threads,
                }
            )

    print(f"{'='*72}")

    # Summary
    if results:
        print(f"\n=== Summary (pool_size={args.pool_size_gb}GB, threads={num_threads}) ===")
        print(json.dumps(results, indent=2))

        # Save results
        thread_tag = f"_t{num_threads}" if num_threads > 1 else ""
        out_file = (
            f"kvcache_bench_pool{args.pool_size_gb}gb"
            f"{'_gpu' if args.use_gpu else '_cpu'}{thread_tag}.json"
        )
        with open(out_file, "w") as f:
            json.dump(
                {
                    "pool_size_gb": args.pool_size_gb,
                    "protocol": args.protocol,
                    "use_gpu": args.use_gpu,
                    "kv_bytes_per_token": args.kv_bytes_per_token,
                    "threads": num_threads,
                    "iterations": args.iterations,
                    "results": results,
                },
                f,
                indent=2,
            )
        print(f"Results saved to {out_file}")


def main():
    args = parse_args()
    if args.mode == "target":
        run_target(args)
    else:
        run_initiator(args)


if __name__ == "__main__":
    main()
