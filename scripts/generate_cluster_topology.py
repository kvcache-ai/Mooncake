import json
import time
import os
import argparse
import socket
import subprocess
import paramiko
from tqdm import tqdm
from itertools import product
from collections import defaultdict, Counter
import numpy as np
from scipy.optimize import linear_sum_assignment


def is_local_host(host):
    return host in ("localhost", "127.0.0.1", socket.gethostname())

def local_exec(command):
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.decode() + result.stderr.decode()

def ssh_exec(host, port, command):
    if is_local_host(host):
        return local_exec(command)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port=port)
    stdin, stdout, stderr = client.exec_command(command)
    stdout.channel.recv_exit_status()
    out = stdout.read().decode()
    err = stderr.read().decode()
    client.close()
    return out + err

def get_machine_id(host, port):
    return ssh_exec(host, port, "cat /etc/machine-id").strip()


# -------------------- Section 1: SSH + RDMA Testing --------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Transfer Engine Cluster Topology Generator")
    parser.add_argument("--src-host", default="localhost", help="Source hostname")
    parser.add_argument("--dst-host", required=True, help="Destination hostname")
    parser.add_argument("--src-port", type=int, default=22, help="SSH port for source host")
    parser.add_argument("--dst-port", type=int, default=22, help="SSH port for destination host")
    parser.add_argument("--sudo", action="store_true", help="Use sudo for remote commands")
    parser.add_argument("--file", default="cluster-topology.json", help="Path of the generated cluster topology file")
    return parser.parse_args()


def list_rdma_devices(host, port, use_sudo):
    cmd = ("sudo " if use_sudo else "") + "ibv_devices | grep -v device | awk '{print $1}'"
    out = ssh_exec(host, port, cmd)
    devs = out.strip().splitlines()

    results = []
    for dev in devs:
        sysfs_cmd = f"cat /sys/class/infiniband/{dev}/device/numa_node"
        if use_sudo:
            sysfs_cmd = "sudo " + sysfs_cmd
        numa_str = ssh_exec(host, port, sysfs_cmd).strip()
        try:
            numa = int(numa_str)
        except:
            numa = -1
        results.append({"name": dev, "numa_node": numa})
    return results


def parse_bandwidth(output):
    for line in output.strip().splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[0].isdigit():
            try:
                return float(parts[3])
            except ValueError:
                return None
    return None


def parse_latency(output):
    for line in output.strip().splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0].isdigit():
            try:
                return float(parts[5])
            except ValueError:
                return None
    return None


def run_rdmatest(src, dst, dev1, dev2, use_sudo):
    prefix = "sudo " if use_sudo else ""

    def numactl_prefix(numa):
        if numa < 0:
            return ""
        return f"numactl --cpunodebind={numa} --membind={numa} "

    cmd_prefix_src = numactl_prefix(dev1["numa_node"]) + prefix
    cmd_prefix_dst = numactl_prefix(dev2["numa_node"]) + prefix

    ssh_exec(dst["host"], dst["port"],
             f"{prefix}pkill ib_write_bw; {cmd_prefix_dst}nohup ib_write_bw --ib-dev={dev2['name']} > /tmp/bw_server.log 2>&1 &")
    time.sleep(0.5)

    bw_output = ssh_exec(src["host"], src["port"],
                         f"{cmd_prefix_src}ib_write_bw {dst['host']} --ib-dev={dev1['name']}")
    bw_val = parse_bandwidth(bw_output)
    if bw_val is None:
        return None

    ssh_exec(dst["host"], dst["port"],
             f"{prefix}pkill ib_read_lat; {cmd_prefix_dst}nohup ib_read_lat --ib-dev={dev2['name']} > /tmp/lat_server.log 2>&1 &")
    time.sleep(0.5)

    lat_output = ssh_exec(src["host"], src["port"],
                          f"{cmd_prefix_src}ib_read_lat {dst['host']} --ib-dev={dev1['name']}")
    lat_val = parse_latency(lat_output)

    return {
        "src_dev": dev1["name"],
        "dst_dev": dev2["name"],
        "src_numa": dev1["numa_node"],
        "dst_numa": dev2["numa_node"],
        "bandwidth": bw_val,
        "latency": lat_val
    }


def load_results(filepath):
    if os.path.exists(filepath):
        with open(filepath) as f:
            return json.load(f)
    return []


def save_results(filepath, results):
    with open(filepath, "w") as f:
        json.dump(results, f, indent=2)


# -------------------- Section 2: Partition Matching --------------------
def build_partition_map(endpoints):
    partition_map = defaultdict(list)
    for ep in endpoints:
        if not np.isfinite(ep.get("latency", float('inf'))):
            continue
        key = f"{ep['src_numa']}-{ep['dst_numa']}"
        partition_map[key].append(ep)
    return partition_map


def solve_partition_group(pairs, allow_partial=False):
    src_devs = sorted(set(ep['src_dev'] for ep in pairs))
    dst_devs = sorted(set(ep['dst_dev'] for ep in pairs))

    N_src = len(src_devs)
    N_dst = len(dst_devs)
    N = max(N_src, N_dst)

    idx_src = {dev: i for i, dev in enumerate(src_devs)}
    idx_dst = {dev: i for i, dev in enumerate(dst_devs)}

    cost = np.full((N, N), 1e9)
    valid = np.zeros((N, N), dtype=bool)
    latency_map = {}

    for ep in pairs:
        i = idx_src[ep['src_dev']]
        j = idx_dst[ep['dst_dev']]
        cost[i, j] = ep['latency']
        valid[i, j] = True
        latency_map[(i, j)] = ep

    finite_costs = cost[np.isfinite(cost)]
    if len(finite_costs) == 0:
        return []

    min_cost = np.min(finite_costs)
    max_cost = np.max(finite_costs)
    norm = (cost - min_cost) / (max_cost - min_cost + 1e-6)

    row_ind, col_ind = linear_sum_assignment(norm)

    matched = []
    for i, j in zip(row_ind, col_ind):
        if i < N_src and j < N_dst and valid[i, j]:
            matched.append(latency_map[(i, j)])
        elif not allow_partial:
            return []

    return matched


def process_host_pair(record):
    endpoints = record.get("endpoints", [])
    partition_map = build_partition_map(endpoints)
    result = {}

    for part_key, part_eps in partition_map.items():
        optimal = solve_partition_group(part_eps)
        if optimal:
            result[part_key] = optimal

        used_src = set(ep['src_dev'] for ep in optimal)
        used_dst = set(ep['dst_dev'] for ep in optimal)
        extras = [ep for ep in part_eps if ep['src_dev'] not in used_src and ep['dst_dev'] not in used_dst]
        extra_opt = solve_partition_group(extras, allow_partial=True)
        if extra_opt:
            result[part_key + "_extra"] = extra_opt

    record['partition_matchings'] = result


# -------------------- Main Orchestration --------------------
def main():
    args = parse_args()

    src_machine_id = get_machine_id(args.src_host, args.src_port)
    dst_machine_id = get_machine_id(args.dst_host, args.dst_port)

    src = {"host": args.src_host, "port": args.src_port}
    dst = {"host": args.dst_host, "port": args.dst_port}
    use_sudo = args.sudo

    result_file = args.file
    all_results = load_results(result_file)

    existing_idx = next((i for i, e in enumerate(all_results)
                         if e["src_host"] == src_machine_id and e["dst_host"] == dst_machine_id),
                        None)

    if existing_idx is not None:
        confirm = input(f"\nEntry already exists for {src_machine_id} → {dst_machine_id}. "
                        f"Do you want to overwrite and re-test? (y = overwrite and retest / n = skip): ").strip().lower()
        if confirm != 'y':
            print("Skipping test and keeping existing result.")
            return
        else:
            print("Overwriting existing entry after re-testing.")

    print(f"Discovering RDMA devices on {src['host']} and {dst['host']}...")
    devices_src = list_rdma_devices(src["host"], src["port"], use_sudo)
    devices_dst = list_rdma_devices(dst["host"], dst["port"], use_sudo)

    total = len(devices_src) * len(devices_dst)
    endpoints = []

    for dev1, dev2 in tqdm(product(devices_src, devices_dst), total=total, desc="Testing", unit="test"):
        result = run_rdmatest(src, dst, dev1, dev2, use_sudo)
        if result:
            endpoints.append(result)

    new_entry = {
        "src_host": src_machine_id,
        "dst_host": dst_machine_id,
        "endpoints": endpoints
    }

    if existing_idx is not None:
        all_results[existing_idx] = new_entry
    else:
        all_results.append(new_entry)

    for record in all_results:
        process_host_pair(record)

    save_results(result_file, all_results)
    print(f"\nRDMA test results written to {result_file}")


if __name__ == "__main__":
    main()
