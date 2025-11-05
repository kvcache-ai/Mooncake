#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import torch
import base64
from kvbench_common import get_local_ip, send_handshake
from mooncake.engine import TransferEngine
from nixl._api import nixl_agent, nixl_agent_config

use_mnnvl = False

def run_server_mooncake(buf_size: int, num_gpus: int, zmq_port: int, mem_type: str):
    te = TransferEngine()
    rc = te.initialize(get_local_ip(), "P2PHANDSHAKE", "rdma", "")
    if rc != 0:
        raise RuntimeError(f"TransferEngine init failed: {rc}")

    buf_addrs = []
    for dev in range(num_gpus):
        if mem_type == "cuda":
            torch.cuda.set_device(dev)
            if use_mnnvl:
                from mooncake.allocator import NVLinkAllocator
                alloc = NVLinkAllocator.get_allocator(f"cuda:{dev}")
                torch.cuda.memory.change_current_allocator(alloc)
            buf = torch.zeros(buf_size // 4, dtype=torch.float32, device=f"cuda:{dev}")
            dev_str = f"cuda:{dev}"
        else:
            buf = torch.zeros(buf_size // 4, dtype=torch.float32, pin_memory=True)
            dev_str = "cpu"
        addr = buf.data_ptr()
        te.register_memory(addr, buf_size)
        buf_addrs.append(addr)
        print(f"[SERVER/RDMA] {dev_str} @ {addr:#x}")

    info = {
        "backend": "mooncake",
        "remote": f"{get_local_ip()}:{te.get_rpc_port()}",
        "buf_addrs": buf_addrs,
        "buf_size": buf_size,
        "num_gpus": num_gpus,
    }
    print(f"[SERVER/RDMA] RPC @ {info['remote']}")
    send_handshake(info, port=zmq_port)


def _b64(s: bytes) -> str:
    return base64.b64encode(s).decode("ascii")


def run_server_nixl(buf_size: int, num_gpus: int, zmq_port: int, mem_type: str):
    agent = nixl_agent("target", nixl_agent_config(backends=["UCX"]))
    buf_addrs = []
    nixl_xfer_descs = []
    for dev in range(num_gpus):
        if mem_type == "cuda":
            torch.cuda.set_device(dev)
            if use_mnnvl:
                from mooncake.allocator import NVLinkAllocator
                alloc = NVLinkAllocator.get_allocator(f"cuda:{dev}")
                torch.cuda.memory.change_current_allocator(alloc)
            buf = torch.zeros(buf_size // 4, dtype=torch.float32, device=f"cuda:{dev}")
            dev_str = f"cuda:{dev}"
        else:
            buf = torch.zeros(buf_size // 4, dtype=torch.float32, pin_memory=True)
            dev_str = "cpu"
        addr = buf.data_ptr()
        reg_descs = agent.get_reg_descs([(addr, buf_size, 0, dev_str)], "DRAM")
        agent.register_memory(reg_descs)

        xfd = agent.get_xfer_descs([(addr, buf_size, 0)], "DRAM")
        nixl_xfer_descs.append(xfd[0] if isinstance(xfd, list) and len(xfd) == 1 else xfd)

        buf_addrs.append(addr)
        print(f"[SERVER/NIXL] {dev_str} @ {addr:#x}")

    descs_meta = agent.get_serialized_descs(nixl_xfer_descs)
    meta = agent.get_agent_metadata()
    info = {
        "backend": "nixl",
        "num_gpus": num_gpus,
        "buf_addrs": buf_addrs,
        "buf_size": buf_size,
        "nixl_xfer_descs": _b64(descs_meta),
        "nixl_meta": _b64(meta),
    }

    print(f"[SERVER/NIXL] export meta & xfer_descs: GPUs={num_gpus}, buf={buf_size}, mem_type={mem_type}")
    send_handshake(info, port=zmq_port)


def main():
    ap = argparse.ArgumentParser("kvbench-server")
    ap.add_argument("--backend", choices=["mooncake", "nixl"], default="mooncake")
    ap.add_argument("--zmq_port", type=int, default=5555)
    ap.add_argument("--num_gpus", type=int, default=8)
    ap.add_argument("--buf_size", type=int, default=512 * 1024 * 1024)
    ap.add_argument("--mem_type", choices=["cuda", "dram"], default="cuda",
                    help="Memory allocation type: 'cuda' for GPU, 'dram' for host DRAM")

    a = ap.parse_args()

    if a.backend == "mooncake":
        run_server_mooncake(a.buf_size, a.num_gpus, a.zmq_port, a.mem_type)
    else:
        run_server_nixl(a.buf_size, a.num_gpus, a.zmq_port, a.mem_type)

if __name__ == "__main__":
    main()
