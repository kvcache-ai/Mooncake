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

import etcd3
import sys
import json
import os
import socket
import subprocess

def discover_nvmeof_targets(nqn, transport, traddr, trsvcid):
    """Discover NVMe-oF targets."""
    cmd = f"nvme discover -t {transport} -a {traddr} -s {trsvcid}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Discovery failed: {result.stderr}")
        return None
    print(f"Discovered NVMe-oF targets:\n{result.stdout}")
    return result.stdout

def connect_nvmeof_target(nqn, transport, traddr, trsvcid):
    """Connect to NVMe-oF target."""
    cmd = f"nvme connect -t {transport} -n {nqn} -a {traddr} -s {trsvcid}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Connection failed: {result.stderr}")
        return None
    print(f"Connected to NVMe-oF target:\n{result.stdout}")
    return result.stdout

def mount_nvme_device(device, mount_point):
    """Mount the NVMe device to a specific mount point."""
    os.makedirs(mount_point, exist_ok=True)
    cmd = f"mount {device} {mount_point}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Mount failed: {result.stderr}")
        return None
    print(f"Mounted {device} on {mount_point}")
    return mount_point

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python mount.py <etcd_host> <segment_name> <file_path> <local_path>")
        sys.exit(1)

    os.environ.pop("HTTP_PROXY", None)
    os.environ.pop("HTTPS_PROXY", None)
    os.environ.pop("http_proxy", None)
    os.environ.pop("https_proxy", None)
    etcd_host = sys.argv[1]
    segment_name = sys.argv[2]
    file_path = sys.argv[3]
    local_path = sys.argv[4]

    local_server_name = socket.gethostname()

    etcd = etcd3.client(host=etcd_host, port=2379)
    value, _ = etcd.get(segment_name)

    segment = json.loads(value)
    buffers = segment["buffers"]
    print(buffers)

    buffer = next((b for b in buffers if b["file_path"] == file_path), None)
    buffer['local_path_map'][local_server_name] = local_path

    etcd.put(segment_name, json.dumps(segment))
    print(etcd.get(segment_name)[0])

    etcd.put(segment_name, json.dumps(segment))
    print(etcd.get(segment_name)[0])

  # TODO: mount the buffer to local_path, currently users should mount buffers manually

