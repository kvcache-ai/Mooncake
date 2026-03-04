#!/usr/bin/env python3
# Copyright 2024 KVCache.AI
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

"""
EFA Latency vs Cache Size Benchmark

Runs transfer_engine_bench on two machines via SSH to measure throughput
for multiple EFA configurations. For each cache size point, an independent
benchmark run measures throughput, then latency = cache_size / throughput
is computed. Running separate measurements per point captures natural
throughput variation between runs.

Produces a chart similar to image/transfer-engine-performance.png.

Usage:
    python3 efa_latency_bench.py
    python3 efa_latency_bench.py --target_host=HOST_A --initiator_host=HOST_B
    python3 efa_latency_bench.py --output=my_chart.png --duration=10
"""

import argparse
import os
import re
import subprocess
import sys
import time


def parse_args():
    parser = argparse.ArgumentParser(
        description="EFA Latency vs Cache Size Benchmark"
    )
    parser.add_argument(
        "--target_host",
        default="ip-172-31-22-204",
        help="Hostname or IP of the target machine (default: ip-172-31-22-204)",
    )
    parser.add_argument(
        "--initiator_host",
        default="ip-172-31-26-160",
        help="Hostname or IP of the initiator machine (default: ip-172-31-26-160)",
    )
    parser.add_argument(
        "--build_dir",
        default="/home/ubuntu/Mooncake-efa/build-efa",
        help="Path to the Mooncake build directory on both machines",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Benchmark duration in seconds per measurement point (default: 10)",
    )
    parser.add_argument(
        "--output",
        default="efa_latency_bench.png",
        help="Output chart filename (default: efa_latency_bench.png)",
    )
    parser.add_argument(
        "--cache_sizes",
        default="1,5,10,20,30,40,50,60,70,80,90,100",
        help="Comma-separated cache sizes in GB (default: 1,5,10,20,...,100)",
    )
    parser.add_argument(
        "--operation",
        default="write",
        choices=["read", "write"],
        help="Transfer operation type (default: write)",
    )
    parser.add_argument(
        "--ssh_user",
        default="ubuntu",
        help="SSH username for remote connections (default: ubuntu)",
    )
    parser.add_argument(
        "--ssh_opts",
        default="-o StrictHostKeyChecking=no -o ConnectTimeout=10",
        help="Additional SSH options",
    )
    parser.add_argument(
        "--annotation_gb",
        type=int,
        default=40,
        help="Cache size (GB) at which to annotate speedup ratio (default: 40)",
    )
    parser.add_argument(
        "--buffer_size",
        type=int,
        default=1073741824,
        help="Buffer size in bytes for bench tool (default: 1073741824 = 1 GiB)",
    )
    return parser.parse_args()


# Benchmark configurations
CONFIGS = [
    {
        "label": "EFA (tuned)",
        "env": {"MC_SLICE_SIZE": "262144"},
        "flags": {
            "threads": 48,
            "block_size": 131072,  # 128KB
            "batch_size": 128,
        },
        "color": "#1f77b4",  # blue
        "marker": "o",
    },
    {
        "label": "EFA (default)",
        "env": {},
        "flags": {
            "threads": 8,
            "block_size": 65536,  # 64KB
            "batch_size": 128,
        },
        "color": "#ff7f0e",  # orange
        "marker": "^",
    },
]


def run_ssh(host, command, user="ubuntu", ssh_opts="", timeout=None):
    """Run a command on a remote host via SSH."""
    ssh_args = [
        "ssh",
        *ssh_opts.split(),
        f"{user}@{host}",
        command,
    ]
    try:
        result = subprocess.run(
            ssh_args, capture_output=True, text=True, timeout=timeout,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"


def run_cmd(cmd, timeout=None):
    """Run a local shell command."""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"


def kill_bench(target_host, user, ssh_opts):
    """Kill any running transfer_engine_bench processes."""
    kill_cmd = (
        "ps aux | grep '[t]ransfer_engine_bench' | awk '{print $2}' "
        "| xargs -r kill 2>/dev/null; sleep 1; echo done"
    )
    run_ssh(target_host, kill_cmd, user, ssh_opts, timeout=15)
    run_cmd(
        "ps aux | grep '[t]ransfer_engine_bench' | awk '{print $2}' "
        "| xargs -r kill 2>/dev/null",
        timeout=5,
    )
    time.sleep(2)


def start_target(target_host, build_dir, buffer_size, user, ssh_opts):
    """Start the target process via SSH. Returns target address or None."""
    bench_bin = os.path.join(
        build_dir, "mooncake-transfer-engine/example/transfer_engine_bench"
    )
    log_file = "/tmp/efa_bench_target.log"

    target_cmd = (
        f"cd {build_dir} && "
        f"env MC_METADATA_SERVER=P2PHANDSHAKE "
        f"{bench_bin} "
        f"--mode=target --protocol=efa --metadata_server=P2PHANDSHAKE "
        f"--buffer_size={buffer_size} "
        f"> {log_file} 2>&1"
    )
    ssh_args = [
        "ssh", "-n",
        *ssh_opts.split(),
        f"{user}@{target_host}",
        target_cmd,
    ]
    subprocess.Popen(
        ssh_args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
    )

    for _ in range(20):
        time.sleep(1)
        rc, stdout, _ = run_ssh(
            target_host,
            f"grep 'listening on' {log_file} 2>/dev/null",
            user, ssh_opts, timeout=10,
        )
        if rc == 0 and "listening on" in stdout:
            match = re.search(r"listening on (\S+:\d+)", stdout)
            if match:
                return match.group(1)

    _, log_out, _ = run_ssh(
        target_host, f"tail -20 {log_file}", user, ssh_opts, timeout=10
    )
    print(f"  Target log:\n{log_out}", file=sys.stderr)
    return None


def run_initiator(initiator_host, build_dir, target_addr, config,
                  buffer_size, duration, operation, user, ssh_opts):
    """Run the initiator benchmark. Returns throughput in GB/s or None."""
    bench_bin = os.path.join(
        build_dir, "mooncake-transfer-engine/example/transfer_engine_bench"
    )
    flags = config["flags"]

    env_parts = ["MC_METADATA_SERVER=P2PHANDSHAKE"]
    for key, val in config.get("env", {}).items():
        env_parts.append(f"{key}={val}")
    env_str = " ".join(env_parts)

    bench_cmd = (
        f"cd {build_dir} && "
        f"{env_str} {bench_bin} "
        f"--mode=initiator --protocol=efa --metadata_server=P2PHANDSHAKE "
        f"--segment_id={target_addr} "
        f"--operation={operation} "
        f"--duration={duration} "
        f"--threads={flags['threads']} "
        f"--block_size={flags['block_size']} "
        f"--batch_size={flags['batch_size']} "
        f"--buffer_size={buffer_size} "
        f"--report_unit=GB "
        f"2>&1"
    )

    timeout = duration + 60
    rc, stdout, stderr = run_ssh(
        initiator_host, bench_cmd, user, ssh_opts, timeout=timeout
    )

    combined = stdout + "\n" + stderr
    match = re.search(r"throughput\s+([\d.]+)\s+GB/s", combined)
    if match:
        return float(match.group(1))

    print(f"  WARNING: Could not parse throughput", file=sys.stderr)
    lines = combined.strip().split("\n")
    for line in lines[-3:]:
        print(f"    {line}", file=sys.stderr)
    return None


def plot_results(results, cache_sizes_gb, annotation_gb, output_path):
    """Generate and save the latency vs cache size chart."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(1, 1, figsize=(7, 5))

    for entry in results:
        ax.plot(
            cache_sizes_gb,
            entry["latencies"],
            color=entry["color"],
            marker=entry["marker"],
            markersize=5,
            linewidth=1.5,
            label=entry["label"],
        )

    ax.set_xlabel("Cache Size (GB)", fontsize=12)
    ax.set_ylabel("Latency (s)", fontsize=12)
    ax.set_xlim(0, max(cache_sizes_gb))
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.3)

    # Speedup annotation
    if len(results) >= 2 and annotation_gb in cache_sizes_gb:
        idx = cache_sizes_gb.index(annotation_gb)
        tuned_lat = results[0]["latencies"][idx]
        default_lat = results[1]["latencies"][idx]

        if tuned_lat > 0:
            speedup = default_lat / tuned_lat
            ax.axvline(x=annotation_gb, color="gray", linestyle="--", alpha=0.5)
            mid_y = (tuned_lat + default_lat) / 2
            ax.annotate(
                "",
                xy=(annotation_gb + 1, tuned_lat),
                xytext=(annotation_gb + 1, default_lat),
                arrowprops=dict(arrowstyle="<->", color="black", lw=1.5),
            )
            ax.text(
                annotation_gb + 3, mid_y,
                f"{speedup:.1f}x",
                fontsize=11, fontweight="bold", va="center",
            )

    ax.text(
        0.02, 0.98, "8 x 400 Gbps EFA NICs",
        transform=ax.transAxes, fontsize=10,
        verticalalignment="top", fontweight="bold",
    )
    ax.legend(fontsize=10, loc="upper left", bbox_to_anchor=(0.0, 0.90))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\nChart saved to: {output_path}")


def main():
    args = parse_args()
    cache_sizes_gb = [float(x) for x in args.cache_sizes.split(",")]

    print("=" * 60)
    print("EFA Latency vs Cache Size Benchmark")
    print("=" * 60)
    print(f"  Target host:    {args.target_host}")
    print(f"  Initiator host: {args.initiator_host}")
    print(f"  Build dir:      {args.build_dir}")
    print(f"  Duration:       {args.duration}s per measurement")
    print(f"  Operation:      {args.operation}")
    print(f"  Buffer size:    {args.buffer_size} bytes")
    print(f"  Cache sizes:    {cache_sizes_gb} GB")
    print(f"  Output:         {args.output}")
    print(f"  Configs:        {len(CONFIGS)}")
    n_runs = len(CONFIGS) * len(cache_sizes_gb)
    print(f"  Total runs:     {n_runs} "
          f"(each {args.duration}s + overhead)")
    print()

    results = []

    for ci, config in enumerate(CONFIGS):
        label = config["label"]
        print(f"[{ci+1}/{len(CONFIGS)}] Config: {label}")
        print(f"  threads={config['flags']['threads']}, "
              f"block_size={config['flags']['block_size']}, "
              f"batch_size={config['flags']['batch_size']}")
        if config.get("env"):
            print(f"  env: {config['env']}")
        print()

        throughputs = []
        latencies = []

        # Start target once for all cache sizes of this config
        kill_bench(args.target_host, args.ssh_user, args.ssh_opts)
        print(f"  Starting target...", end="", flush=True)
        target_addr = start_target(
            args.target_host, args.build_dir, args.buffer_size,
            args.ssh_user, args.ssh_opts,
        )
        if not target_addr:
            print(" FAILED")
            continue
        print(f" ready ({target_addr})")
        print()

        for si, cache_gb in enumerate(cache_sizes_gb):
            tag = f"  [{si+1}/{len(cache_sizes_gb)}] {cache_gb:6.0f} GB"
            print(f"{tag} ...", end="", flush=True)

            tp = run_initiator(
                args.initiator_host, args.build_dir,
                target_addr, config, args.buffer_size,
                args.duration, args.operation,
                args.ssh_user, args.ssh_opts,
            )

            if tp is None:
                print(" FAILED")
                throughputs.append(None)
                latencies.append(None)
                continue

            lat = cache_gb / tp
            throughputs.append(tp)
            latencies.append(lat)
            print(f" {tp:7.2f} GB/s  lat={lat:.3f}s")

        # Cleanup
        kill_bench(args.target_host, args.ssh_user, args.ssh_opts)

        valid = [t for t in throughputs if t is not None]
        if not valid:
            print(f"  ERROR: No results for {label}\n", file=sys.stderr)
            continue

        results.append({
            "label": label,
            "throughputs": throughputs,
            "latencies": latencies,
            "color": config["color"],
            "marker": config["marker"],
        })
        print()

    if not results:
        print("ERROR: No results collected.", file=sys.stderr)
        sys.exit(1)

    # Summary table
    print("=" * 60)
    print("Results Summary")
    print("=" * 60)
    header = f"{'Cache(GB)':>10}"
    for r in results:
        header += f"  {r['label']:>24}"
    print(header)
    print("-" * len(header))
    for i, cache_gb in enumerate(cache_sizes_gb):
        row = f"{cache_gb:>10.0f}"
        for r in results:
            tp = r["throughputs"][i]
            lat = r["latencies"][i]
            if tp is not None:
                row += f"  {tp:7.2f} GB/s  lat={lat:.3f}s"
            else:
                row += f"  {'N/A':>24}"
        print(row)
    print()

    # Plot only valid points
    plot_cache = []
    plot_data = [{**r, "latencies": []} for r in results]
    for i, cache_gb in enumerate(cache_sizes_gb):
        if all(r["latencies"][i] is not None for r in results):
            plot_cache.append(cache_gb)
            for j in range(len(results)):
                plot_data[j]["latencies"].append(results[j]["latencies"][i])

    plot_results(plot_data, plot_cache, args.annotation_gb, args.output)


if __name__ == "__main__":
    main()
