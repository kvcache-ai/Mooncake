#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket, zmq, json, time
from typing import Tuple, List, Dict
import os

# ---------- Networking ----------
def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def parse_remote_and_port(target_addr: str, default_port: int) -> Tuple[str, int]:
    if not target_addr:
        return "127.0.0.1", default_port
    if ":" in target_addr:
        host, port_str = target_addr.split(":")
        return host.strip(), int(port_str)
    return target_addr.strip(), default_port

# ---------- Memory block utilities ----------
def aligned_offset(offset: int, align: int) -> int:
    return ((offset + align - 1) // align) * align

def prepare_noncontiguous_blocks(
    src_base: int,
    dst_base: int,
    nope_block_size: int,
    rope_block_size: int,
    num_blocks: int,
    src_buf_size: int,
    dst_buf_size: int,
    align: int = 4096,
    gap_ratio: float = 0.10,
):
    buffers, peer_buffers, lengths = [], [], []
    local_ptr, remote_ptr = src_base, dst_base
    src_limit, dst_limit = src_base + src_buf_size, dst_base + dst_buf_size

    def add_block(block_size: int):
        nonlocal local_ptr, remote_ptr
        if local_ptr + block_size > src_limit or remote_ptr + block_size > dst_limit:
            raise ValueError(
                f"Block exceeds capacity: local {local_ptr:#x}, remote {remote_ptr:#x}"
            )
        buffers.append(local_ptr)
        peer_buffers.append(remote_ptr)
        lengths.append(block_size)
        gap = int(block_size * gap_ratio)
        local_ptr  = aligned_offset(local_ptr  + block_size + gap, align)
        remote_ptr = aligned_offset(remote_ptr + block_size + gap, align)
        # local_ptr  = local_ptr + block_size
        # remote_ptr = remote_ptr + block_size

    for _ in range(num_blocks): add_block(nope_block_size)
    for _ in range(num_blocks): add_block(rope_block_size)
    return buffers, peer_buffers, lengths

# ---------- ZMQ handshake ----------
def send_handshake(info: dict, port: int = 5555):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.bind(f"tcp://*:{port}")
    print(f"[ZMQ] Handshake server tcp://*:{port}")
    try:
        while True:
            _ = sock.recv_string()
            sock.send_json(info)
    except KeyboardInterrupt:
        print("[ZMQ] Handshake exiting.")
    finally:
        sock.close(linger=0); ctx.term()

def recv_handshake(target_addr: str, default_port: int = 5555, timeout: int = 10) -> dict:
    host, port = parse_remote_and_port(target_addr, default_port)
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.connect(f"tcp://{host}:{port}")
    sock.RCVTIMEO = timeout * 1000
    sock.send_string("HELLO")
    info = sock.recv_json()
    sock.close(); ctx.term()
    return info

# ---------- Trace ----------
def load_trace(path: str) -> List[Dict]:
    evs = []
    with open(path) as f:
        for line in f:
            if line.strip():
                evs.append(json.loads(line))
    evs.sort(key=lambda e: e["timestamp"])
    print(f"[TRACE] {len(evs)} events loaded from {path}")
    return evs

# ---------- Dispatcher ----------
class Dispatcher:
    def __init__(self, policy: str, num_local_tp: int, num_remote_tp: int):
        self.policy = policy
        self.num_local = num_local_tp
        self.num_remote = num_remote_tp
        self._rr_local = 0
        self._rr_remote = 0

    def choose_local_tp(self, ev: Dict, idx: int) -> int:
        if self.policy == "hash":
            key = ev.get("hash_ids", [idx])[0] if ev.get("hash_ids") else idx
            return abs(hash(key)) % self.num_local
        elif self.policy == "roundrobin":
            tp = self._rr_local
            self._rr_local = (self._rr_local + 1) % self.num_local
            return tp
        return os.urandom(2)[0] % self.num_local  # random

    def choose_remote_tp(self, ev: Dict, idx: int) -> int:
        if self.policy == "hash":
            key = ev.get("hash_ids", [idx])[0] if ev.get("hash_ids") else idx
            return abs(hash(("remote", key))) % self.num_remote
        elif self.policy == "roundrobin":
            tp = self._rr_remote
            self._rr_remote = (self._rr_remote + 1) % self.num_remote
            return tp
        return os.urandom(2)[0] % self.num_remote

# ---------- small stats ----------
def pct(vals, p):
    if not vals: return 0.0
    srt = sorted(vals)
    k = (len(vals) - 1) * p / 100.0
    lo = int(k); hi = min(lo + 1, len(srt)-1); frac = k - lo
    return srt[lo]*(1-frac) + srt[hi]*frac
