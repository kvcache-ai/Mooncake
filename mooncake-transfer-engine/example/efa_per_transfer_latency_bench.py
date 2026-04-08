#!/usr/bin/env python3
"""
EFA Per-Transfer Latency Benchmark

Measures single-transfer latency by running transfer_engine_bench with
threads=1, batch_size=1 across multiple block sizes. This isolates the
per-request overhead (slice creation, spinlock, atomic ops, MR lookup)
that the P0 NIC-striping optimization aims to eliminate.

Usage:
    python3 efa_per_transfer_latency_bench.py \
        --target_host=HOST_A --initiator_host=HOST_B \
        --build_dir=/path/to/build \
        --ssh_opts="-i /path/to/key.pem"
"""

import argparse
import os
import re
import subprocess
import sys
import time


def parse_args():
    parser = argparse.ArgumentParser(
        description="EFA Per-Transfer Latency Benchmark"
    )
    parser.add_argument("--target_host", required=True)
    parser.add_argument("--initiator_host", required=True)
    parser.add_argument(
        "--build_dir",
        default="/opt/dlami/nvme/Mooncake/build",
    )
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--operation", default="write", choices=["read", "write"])
    parser.add_argument("--ssh_user", default="ubuntu")
    parser.add_argument(
        "--ssh_opts",
        default="-o StrictHostKeyChecking=no -o ConnectTimeout=10",
    )
    parser.add_argument(
        "--block_sizes",
        default="65536,131072,262144,524288,1048576,2097152,4194304,8388608,16777216",
        help="Comma-separated block sizes in bytes",
    )
    parser.add_argument("--threads", type=int, default=1, help="Number of threads")
    parser.add_argument("--batch_size", type=int, default=1, help="Batch size")
    parser.add_argument(
        "--env", action="append", default=[],
        help="Environment variables to pass to remote bench (e.g. --env MC_EFA_STRIPING_THRESHOLD=67108864)",
    )
    parser.add_argument("--output", default=None, help="Output file for results")
    return parser.parse_args()


def run_ssh(host, command, user, ssh_opts, timeout=None):
    ssh_args = ["ssh", *ssh_opts.split(), f"{user}@{host}", command]
    try:
        result = subprocess.run(ssh_args, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"


def kill_bench(host, user, ssh_opts):
    cmd = (
        "ps aux | grep '[t]ransfer_engine_bench' | awk '{print $2}' "
        "| xargs -r kill 2>/dev/null; sleep 1; echo done"
    )
    run_ssh(host, cmd, user, ssh_opts, timeout=15)
    time.sleep(2)


def start_target(host, build_dir, user, ssh_opts):
    bench_bin = os.path.join(
        build_dir, "mooncake-transfer-engine/example/transfer_engine_bench"
    )
    log_file = "/tmp/efa_latency_target.log"
    target_cmd = (
        f"cd {build_dir} && "
        f"{bench_bin} "
        f"--mode=target --protocol=efa --metadata_server=P2PHANDSHAKE "
        f"> {log_file} 2>&1"
    )
    ssh_args = ["ssh", "-n", *ssh_opts.split(), f"{user}@{host}", target_cmd]
    subprocess.Popen(
        ssh_args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
    )
    for _ in range(20):
        time.sleep(1)
        rc, stdout, _ = run_ssh(
            host, f"grep 'listening on' {log_file} 2>/dev/null",
            user, ssh_opts, timeout=10,
        )
        if rc == 0 and "listening on" in stdout:
            match = re.search(r"listening on (\S+:\d+)", stdout)
            if match:
                return match.group(1)
    return None


def run_single_bench(host, build_dir, target_addr, block_size,
                     duration, operation, user, ssh_opts,
                     threads=1, batch_size=1, env_vars=None):
    """Run bench with configurable threads and batch_size."""
    bench_bin = os.path.join(
        build_dir, "mooncake-transfer-engine/example/transfer_engine_bench"
    )
    env_prefix = " ".join(env_vars) + " " if env_vars else ""
    bench_cmd = (
        f"cd {build_dir} && "
        f"{env_prefix}"
        f"{bench_bin} "
        f"--mode=initiator --protocol=efa --metadata_server=P2PHANDSHAKE "
        f"--segment_id={target_addr} "
        f"--operation={operation} "
        f"--duration={duration} "
        f"--threads={threads} "
        f"--block_size={block_size} "
        f"--batch_size={batch_size} "
        f"2>&1"
    )
    timeout = duration + 60
    rc, stdout, stderr = run_ssh(host, bench_cmd, user, ssh_opts, timeout=timeout)
    combined = stdout + "\n" + stderr

    # Parse throughput
    match = re.search(r"throughput\s+([\d.]+)\s+GB/s", combined)
    if match:
        return float(match.group(1))

    # Try MB/s
    match = re.search(r"throughput\s+([\d.]+)\s+MB/s", combined)
    if match:
        return float(match.group(1)) / 1024.0

    print(f"    WARNING: Could not parse throughput", file=sys.stderr)
    for line in combined.strip().split("\n")[-3:]:
        print(f"      {line}", file=sys.stderr)
    return None


def format_size(size_bytes):
    if size_bytes >= 1048576:
        return f"{size_bytes / 1048576:.0f}MB"
    return f"{size_bytes / 1024:.0f}KB"


def main():
    args = parse_args()
    block_sizes = [int(x) for x in args.block_sizes.split(",")]

    print("=" * 70)
    print("EFA Per-Transfer Latency Benchmark")
    print("=" * 70)
    print(f"  Target:     {args.target_host}")
    print(f"  Initiator:  {args.initiator_host}")
    print(f"  Build dir:  {args.build_dir}")
    print(f"  Duration:   {args.duration}s per point")
    print(f"  Operation:  {args.operation}")
    print(f"  Mode:       threads={args.threads}, batch_size={args.batch_size}")
    print(f"  Block sizes: {[format_size(b) for b in block_sizes]}")
    print()

    # Start target
    kill_bench(args.target_host, args.ssh_user, args.ssh_opts)
    print("Starting target...", end="", flush=True)
    target_addr = start_target(
        args.target_host, args.build_dir, args.ssh_user, args.ssh_opts
    )
    if not target_addr:
        print(" FAILED")
        sys.exit(1)
    print(f" ready ({target_addr})")
    print()

    results = []
    for i, block_size in enumerate(block_sizes):
        tag = format_size(block_size)
        print(f"  [{i+1}/{len(block_sizes)}] {tag:>6} ...", end="", flush=True)

        tp = run_single_bench(
            args.initiator_host, args.build_dir, target_addr,
            block_size, args.duration, args.operation,
            args.ssh_user, args.ssh_opts,
            threads=args.threads, batch_size=args.batch_size,
            env_vars=args.env,
        )

        if tp is None or tp == 0:
            print(" FAILED")
            results.append((block_size, None, None))
            continue

        # latency = block_size / throughput
        tp_bytes = tp * 1e9  # GB/s -> bytes/s
        latency_us = (block_size / tp_bytes) * 1e6  # microseconds
        results.append((block_size, tp, latency_us))
        print(f" {tp:7.2f} GB/s  latency={latency_us:8.1f} us")

    # Cleanup
    kill_bench(args.target_host, args.ssh_user, args.ssh_opts)

    # Summary
    print()
    print("=" * 70)
    print("Results Summary")
    print("=" * 70)
    print(f"{'Block Size':>12}  {'Throughput':>12}  {'Latency (us)':>14}")
    print("-" * 42)
    for block_size, tp, lat in results:
        tag = format_size(block_size)
        if tp is not None:
            print(f"{tag:>12}  {tp:>9.2f} GB/s  {lat:>11.1f} us")
        else:
            print(f"{tag:>12}  {'N/A':>12}  {'N/A':>14}")

    # Write output file
    if args.output:
        with open(args.output, "w") as f:
            f.write(f"# EFA Per-Transfer Latency Benchmark\n")
            f.write(f"# Operation: {args.operation}\n")
            f.write(f"# Duration: {args.duration}s per point\n")
            f.write(f"# Mode: threads={args.threads}, batch_size={args.batch_size}\n")
            f.write(f"#\n")
            f.write(f"{'block_bytes':>12}  {'block_size':>10}  {'gbps':>10}  {'latency_us':>12}\n")
            for block_size, tp, lat in results:
                tag = format_size(block_size)
                if tp is not None:
                    f.write(f"{block_size:>12}  {tag:>10}  {tp:>10.2f}  {lat:>12.1f}\n")
                else:
                    f.write(f"{block_size:>12}  {tag:>10}  {'N/A':>10}  {'N/A':>12}\n")
        print(f"\nResults saved to: {args.output}")


if __name__ == "__main__":
    main()
