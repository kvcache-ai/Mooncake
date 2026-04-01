#!/usr/bin/env python3
# Copyright Mooncake — RealClient stress workload (Python port of real_client_stress_workload_test.cpp)
"""Mooncake RealClient multi-workload stress test with optional orchestrated preload/read phases."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from mooncake.store import (
    MooncakeDistributedStore,
    ReadRouteConfig,
    ReplicateConfig,
    WriteRouteRequestConfig,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("real_client_stress_workload")

_rw_mismatch_lock = threading.Lock()
_rw_mismatch_logged = False


def generate_key(node_id: int, idx: int) -> str:
    return f"node_{node_id}_obj_{idx}"


def build_write_config(client_mode: str, strict_visibility: bool = False):
    if client_mode == "p2p":
        cfg = WriteRouteRequestConfig()
        cfg.max_candidates = 1
        cfg.allow_local = True
        cfg.prefer_local = True
        cfg.early_return = not strict_visibility
        return cfg
    cfg = ReplicateConfig()
    cfg.replica_num = 1
    return cfg


def build_preload_write_config(client_mode: str):
    if client_mode == "p2p":
        cfg = WriteRouteRequestConfig()
        cfg.max_candidates = 1
        cfg.allow_local = True
        cfg.prefer_local = True
        cfg.early_return = True
        return cfg
    cfg = ReplicateConfig()
    cfg.replica_num = 1
    return cfg


def calculate_percentiles(latencies: List[float]) -> Tuple[float, float, float, float]:
    if not latencies:
        return 0.0, 0.0, 0.0, 0.0
    p50, p80, p95, p99 = np.percentile(latencies, [50, 80, 95, 99])
    return float(p50), float(p80), float(p95), float(p99)


def dump_latencies(path: str, **arrays: List[float]):
    """Save named latency arrays to a .npz file."""
    if not path or not arrays:
        return
    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        np.savez(path, **{k: np.array(v, dtype=np.float64) for k, v in arrays.items() if v})
        logger.info("Dumped latencies to %s", path)
    except Exception as e:
        logger.warning("Failed to dump latencies to %s: %s", path, e)

@dataclass
class OperationResult:
    latency_us: float = 0.0
    success: bool = False
    expected_remote: bool = False
    query_success: bool = False


@dataclass
class ThreadStats:
    reads: List[OperationResult] = field(default_factory=list)
    total_reads: int = 0
    successful_reads: int = 0
    query_failures: int = 0
    get_failures: int = 0
    local_reads: int = 0
    remote_reads: int = 0
    local_successful_reads: int = 0
    remote_successful_reads: int = 0
    successful_read_time_us: float = 0.0


@dataclass
class MixedThreadStats:
    read_latencies_us: List[float] = field(default_factory=list)
    write_latencies_us: List[float] = field(default_factory=list)
    read_attempts: int = 0
    write_attempts: int = 0
    successful_reads: int = 0
    successful_writes: int = 0
    exist_calls: int = 0
    exist_failures: int = 0
    read_failures: int = 0
    write_failures: int = 0
    correctness_failures: int = 0
    successful_read_time_us: float = 0.0
    successful_write_time_us: float = 0.0


@dataclass
class ConcurrentWriteThreadStats:
    write_latencies_us: List[float] = field(default_factory=list)
    write_attempts: int = 0
    successful_writes: int = 0
    write_failures: int = 0
    successful_write_time_us: float = 0.0


@dataclass
class ConcurrentWriteResult:
    thread_stats: List[ConcurrentWriteThreadStats] = field(default_factory=list)
    successful_keys: List[str] = field(default_factory=list)
    write_attempts: int = 0
    successful_writes: int = 0
    write_failures: int = 0
    successful_write_time_us: float = 0.0
    success_rate: float = 0.0
    write_ops_per_sec: float = 0.0
    write_mb_per_sec: float = 0.0
    p50: float = 0.0
    p80: float = 0.0
    p95: float = 0.0
    p99: float = 0.0


def _normalize_tiered_config_json(raw: str) -> str:
    raw = raw.strip().lstrip("\ufeff")
    return json.dumps(json.loads(raw), separators=(",", ":"))


def _resolve_tiered_config_json(args: argparse.Namespace) -> str:
    env_val = os.environ.get("MOONCAKE_TIERED_CONFIG")
    if env_val is not None:
        try:
            return _normalize_tiered_config_json(env_val)
        except json.JSONDecodeError as e:
            logger.warning(
                "MOONCAKE_TIERED_CONFIG is not valid JSON (%s); using --tiered_backend_config.",
                e,
            )
    return _normalize_tiered_config_json(args.tiered_backend_config)


def initialize_store(args: argparse.Namespace) -> MooncakeDistributedStore:
    store = MooncakeDistributedStore()
    tier_json = _resolve_tiered_config_json(args)
    rdma = args.device_names or ""

    if args.client_mode == "p2p":
        rc = store.setup_p2p_real_client(
            args.local_hostname,
            args.metadata_connection_string,
            tier_json,
            args.local_buffer_size,
            args.protocol,
            rdma,
            args.master_address,
            args.client_rpc_port,
            args.rpc_thread_num,
        )
    else:
        rc = store.setup(
            args.local_hostname,
            args.metadata_connection_string,
            args.global_segment_size,
            args.local_buffer_size,
            args.protocol,
            rdma,
            args.master_address,
        )
    if rc != 0:
        raise RuntimeError(f"Store setup failed, rc={rc}")
    return store


def print_read_results(thread_stats: List[ThreadStats], duration_s: float, args: argparse.Namespace):
    total_reads = successful_reads = query_failures = get_failures = 0
    local_reads = remote_reads = local_success = remote_success = 0
    all_latencies: List[float] = []
    local_latencies: List[float] = []
    remote_latencies: List[float] = []

    for st in thread_stats:
        total_reads += st.total_reads
        successful_reads += st.successful_reads
        query_failures += st.query_failures
        get_failures += st.get_failures
        local_reads += st.local_reads
        remote_reads += st.remote_reads
        local_success += st.local_successful_reads
        remote_success += st.remote_successful_reads
        for op in st.reads:
            if not op.success:
                continue
            all_latencies.append(op.latency_us)
            if op.expected_remote:
                remote_latencies.append(op.latency_us)
            else:
                local_latencies.append(op.latency_us)

    all_p50, all_p80, all_p95, all_p99 = calculate_percentiles(all_latencies)
    lp50, lp80, lp95, lp99 = calculate_percentiles(local_latencies)
    rp50, rp80, rp95, rp99 = calculate_percentiles(remote_latencies)
    successful_read_time_us = sum(st.successful_read_time_us for st in thread_stats)

    reads_per_sec = (
        successful_reads * 1e6 / successful_read_time_us if successful_read_time_us > 0 else 0.0
    )
    data_mb_per_sec = (
        (successful_reads * args.value_size) / ((successful_read_time_us / 1e6) * 1024 * 1024)
        if successful_read_time_us > 0
        else 0.0
    )

    logger.info("=== RealClient Multi-Node Stress Results ===")
    logger.info("Node ID: %s", args.node_id)
    logger.info("Duration(s): %s", duration_s)
    logger.info("Threads: %s", args.num_threads)
    logger.info("Value size(bytes): %s", args.value_size)
    logger.info("Total reads: %s", total_reads)
    logger.info("Successful reads: %s", successful_reads)
    logger.info("Query failures: %s", query_failures)
    logger.info("Get failures: %s", get_failures)
    logger.info(
        "Success rate(%%): %s",
        0.0 if total_reads == 0 else 100.0 * successful_reads / total_reads,
    )
    logger.info("Local reads: %s, local successful reads: %s", local_reads, local_success)
    logger.info("Remote reads: %s, remote successful reads: %s", remote_reads, remote_success)
    logger.info("Read active time(s): %s", successful_read_time_us / 1e6)
    wall_clock_reads_per_sec = successful_reads / duration_s if duration_s > 0 else 0.0
    wall_clock_data_mb_per_sec = (
        (successful_reads * args.value_size) / (duration_s * 1024 * 1024)
        if duration_s > 0
        else 0.0
    )

    logger.info("Reads/sec (by read time): %s", reads_per_sec)
    logger.info("Data throughput(MB/s, by read time): %s", data_mb_per_sec)
    logger.info("Reads/sec (wall-clock): %s", wall_clock_reads_per_sec)
    logger.info("Data throughput(MB/s, wall-clock): %s", wall_clock_data_mb_per_sec)
    logger.info(
        "All latency(us) p50/p80/p95/p99 = %s/%s/%s/%s", all_p50, all_p80, all_p95, all_p99
    )
    logger.info(
        "Local latency(us) p50/p80/p95/p99 = %s/%s/%s/%s", lp50, lp80, lp95, lp99
    )
    logger.info(
        "Remote latency(us) p50/p80/p95/p99 = %s/%s/%s/%s", rp50, rp80, rp95, rp99
    )

    success_rate = 0.0 if total_reads == 0 else 100.0 * successful_reads / total_reads

    if args.latency_dump_file:
        dump_latencies(args.latency_dump_file, all=all_latencies, local=local_latencies, remote=remote_latencies)

    return {
        "workload": "preload_then_read",
        "node_id": args.node_id,
        "duration_s": duration_s,
        "num_threads": args.num_threads,
        "value_size": args.value_size,
        "total_reads": total_reads,
        "successful_reads": successful_reads,
        "success_rate": success_rate,
        "query_failures": query_failures,
        "get_failures": get_failures,
        "local_reads": local_reads,
        "local_successful_reads": local_success,
        "remote_reads": remote_reads,
        "remote_successful_reads": remote_success,
        "reads_per_sec_by_read_time": reads_per_sec,
        "data_mb_per_sec_by_read_time": data_mb_per_sec,
        "wall_clock_reads_per_sec": wall_clock_reads_per_sec,
        "wall_clock_data_mb_per_sec": wall_clock_data_mb_per_sec,
        "all_latency_p50_us": all_p50,
        "all_latency_p80_us": all_p80,
        "all_latency_p95_us": all_p95,
        "all_latency_p99_us": all_p99,
        "local_latency_p50_us": lp50,
        "local_latency_p80_us": lp80,
        "local_latency_p95_us": lp95,
        "local_latency_p99_us": lp99,
        "remote_latency_p50_us": rp50,
        "remote_latency_p80_us": rp80,
        "remote_latency_p95_us": rp95,
        "remote_latency_p99_us": rp99,
    }

def preload_keys(store: MooncakeDistributedStore, args: argparse.Namespace) -> bool:
    buf = np.zeros(args.value_size, dtype=np.uint8)
    ptr = buf.ctypes.data
    if store.register_buffer(ptr, args.value_size) != 0:
        logger.error("register_buffer failed in preload")
        return False
    write_cfg = build_preload_write_config(args.client_mode)
    try:
        for i in range(args.key_count):
            key = generate_key(args.node_id, i)
            buf.fill(ord("A") + (i % 26))
            rc = store.put_from(key, ptr, args.value_size, write_cfg)
            if rc != 0:
                logger.error("put_from failed for key=%s, rc=%s", key, rc)
                return False
    finally:
        store.unregister_buffer(ptr)
    logger.info(
        "Preload complete, node=%s, keys=%s", args.node_id, args.key_count
    )
    logger.info(
        "=== Phase preload_done node_id=%s keys=%s ===",
        args.node_id,
        args.key_count,
    )
    return True


def stress_read_worker(
    store: MooncakeDistributedStore,
    args: argparse.Namespace,
    thread_id: int,
    remote_node_ids: List[int],
    stats: ThreadStats,
):
    buf = np.zeros(args.value_size, dtype=np.uint8)
    ptr = buf.ctypes.data
    if store.register_buffer(ptr, args.value_size) != 0:
        logger.error("Thread %s: register_buffer failed", thread_id)
        return

    rng = random.Random(args.random_seed + thread_id)
    read_cfg = ReadRouteConfig()
    has_remote = len(remote_node_ids) > 0
    total_remote_keys = len(remote_node_ids) * args.key_count if has_remote else 0
    total_ops = args.warmup_ops + args.test_operation_nums

    for i in range(total_ops):
        choose_remote = has_remote and (rng.random() < args.remote_read_ratio)
        if choose_remote and total_remote_keys > 0:
            gidx = rng.randrange(total_remote_keys)
            remote_slot = gidx // args.key_count
            remote_key_idx = gidx % args.key_count
            target_node = remote_node_ids[remote_slot]
            key_idx = remote_key_idx
        else:
            target_node = args.node_id
            key_idx = rng.randrange(args.key_count)
        key = generate_key(target_node, key_idx)

        t0 = time.perf_counter()
        size = store.get_into(key, ptr, args.value_size, read_cfg)
        t1 = time.perf_counter()
        latency_us = (t1 - t0) * 1e6

        if i >= args.warmup_ops:
            op = OperationResult()
            op.latency_us = latency_us
            op.expected_remote = choose_remote
            op.success = size >= 0
            op.query_success = op.success
            stats.reads.append(op)
            stats.total_reads += 1
            if choose_remote:
                stats.remote_reads += 1
            else:
                stats.local_reads += 1
            if op.success:
                stats.successful_reads += 1
                stats.successful_read_time_us += latency_us
                if choose_remote:
                    stats.remote_successful_reads += 1
                else:
                    stats.local_successful_reads += 1
            else:
                stats.get_failures += 1

    store.unregister_buffer(ptr)


def stress_read(store: MooncakeDistributedStore, args: argparse.Namespace) -> Dict[str, Any]:
    remote_node_ids = [n for n in range(1, args.num_nodes + 1) if n != args.node_id]
    logger.info(
        "Stress-read config: node_id=%s, num_nodes=%s, local_keys=%s, remote_nodes=%s",
        args.node_id,
        args.num_nodes,
        args.key_count,
        len(remote_node_ids),
    )
    logger.info("=== Phase read_started node_id=%s ===", args.node_id)

    thread_stats = [ThreadStats() for _ in range(args.num_threads)]
    threads: List[threading.Thread] = []
    t0 = time.perf_counter()
    for i in range(args.num_threads):
        th = threading.Thread(
            target=stress_read_worker,
            args=(store, args, i, remote_node_ids, thread_stats[i]),
            name=f"stress-read-{i}",
        )
        threads.append(th)
        th.start()
    for th in threads:
        th.join()
    duration_s = time.perf_counter() - t0
    return print_read_results(thread_stats, duration_s, args)

def print_mixed_results(thread_stats: List[MixedThreadStats], title: str, latency_dump_file: str = "") -> Dict[str, Any]:
    read_attempts = write_attempts = successful_reads = successful_writes = 0
    exist_calls = exist_failures = read_failures = write_failures = correctness_failures = 0
    successful_read_time_us = successful_write_time_us = 0.0
    read_latencies: List[float] = []
    write_latencies: List[float] = []

    for st in thread_stats:
        read_attempts += st.read_attempts
        write_attempts += st.write_attempts
        successful_reads += st.successful_reads
        successful_writes += st.successful_writes
        exist_calls += st.exist_calls
        exist_failures += st.exist_failures
        read_failures += st.read_failures
        write_failures += st.write_failures
        correctness_failures += st.correctness_failures
        successful_read_time_us += st.successful_read_time_us
        successful_write_time_us += st.successful_write_time_us
        read_latencies.extend(st.read_latencies_us)
        write_latencies.extend(st.write_latencies_us)

    rp50, rp80, rp95, rp99 = calculate_percentiles(read_latencies)
    wp50, wp80, wp95, wp99 = calculate_percentiles(write_latencies)
    reads_per_sec = (
        successful_reads * 1e6 / successful_read_time_us if successful_read_time_us > 0 else 0.0
    )
    writes_per_sec = (
        successful_writes * 1e6 / successful_write_time_us
        if successful_write_time_us > 0
        else 0.0
    )

    logger.info("=== %s ===", title)
    logger.info(
        "read_attempts=%s, successful_reads=%s, read_failures=%s",
        read_attempts,
        successful_reads,
        read_failures,
    )
    logger.info(
        "write_attempts=%s, successful_writes=%s, write_failures=%s",
        write_attempts,
        successful_writes,
        write_failures,
    )
    logger.info(
        "exist_calls=%s, exist_failures=%s, correctness_failures=%s",
        exist_calls,
        exist_failures,
        correctness_failures,
    )
    logger.info("Read throughput(ops/s, by read time)=%s", reads_per_sec)
    logger.info("Write throughput(ops/s, by write time)=%s", writes_per_sec)
    logger.info("Read latency(us) p50/p80/p95/p99 = %s/%s/%s/%s", rp50, rp80, rp95, rp99)
    logger.info("Write latency(us) p50/p80/p95/p99 = %s/%s/%s/%s", wp50, wp80, wp95, wp99)

    if latency_dump_file:
        dump_latencies(latency_dump_file, read=read_latencies, write=write_latencies)

    return {
        "workload": title,
        "read_attempts": read_attempts,
        "successful_reads": successful_reads,
        "read_failures": read_failures,
        "write_attempts": write_attempts,
        "successful_writes": successful_writes,
        "write_failures": write_failures,
        "exist_calls": exist_calls,
        "exist_failures": exist_failures,
        "correctness_failures": correctness_failures,
        "reads_per_sec_by_read_time": reads_per_sec,
        "writes_per_sec_by_write_time": writes_per_sec,
        "read_latency_p50_us": rp50,
        "read_latency_p80_us": rp80,
        "read_latency_p95_us": rp95,
        "read_latency_p99_us": rp99,
        "write_latency_p50_us": wp50,
        "write_latency_p80_us": wp80,
        "write_latency_p95_us": wp95,
        "write_latency_p99_us": wp99,
    }



def op_sequence_worker(
    store: MooncakeDistributedStore,
    args: argparse.Namespace,
    thread_id: int,
    remote_node_ids: List[int],
    stats: MixedThreadStats,
):
    buf = np.zeros(args.value_size, dtype=np.uint8)
    ptr = buf.ctypes.data
    if store.register_buffer(ptr, args.value_size) != 0:
        logger.error("Thread %s: register_buffer failed", thread_id)
        stats.correctness_failures += 1
        return
    write_cfg = build_write_config(args.client_mode, strict_visibility=False)
    read_cfg = ReadRouteConfig()
    rng = random.Random(args.random_seed + 97 * thread_id)
    has_remote = len(remote_node_ids) > 0
    total_remote_keys = len(remote_node_ids) * args.key_count if has_remote else 0

    for i in range(args.warmup_ops):
        choose_remote = has_remote and (rng.random() < args.remote_read_ratio)
        if choose_remote and total_remote_keys > 0:
            gidx = rng.randrange(total_remote_keys)
            target_node = remote_node_ids[gidx // args.key_count]
            key_idx = gidx % args.key_count
        else:
            target_node = args.node_id
            key_idx = rng.randrange(args.key_count)
        key = generate_key(target_node, key_idx)
        exist_before = store.is_exist(key)
        if exist_before < 0:
            continue
        if exist_before == 1:
            store.get_into(key, ptr, args.value_size, read_cfg)
        else:
            buf.fill(ord("a") + ((thread_id + i) % 26))
            store.put_from(key, ptr, args.value_size, write_cfg)

    for i in range(args.test_operation_nums):
        choose_remote = has_remote and (rng.random() < args.remote_read_ratio)
        if choose_remote and total_remote_keys > 0:
            gidx = rng.randrange(total_remote_keys)
            remote_slot = gidx // args.key_count
            remote_key_idx = gidx % args.key_count
            target_node = remote_node_ids[remote_slot]
            key_idx = remote_key_idx
        else:
            target_node = args.node_id
            key_idx = rng.randrange(args.key_count)
        key = generate_key(target_node, key_idx)

        done = False
        for round_ in range(args.op_sequence_max_rounds):
            if done:
                break
            stats.exist_calls += 1
            exist_before = store.is_exist(key)
            if exist_before < 0:
                stats.exist_failures += 1
                continue
            if exist_before == 1:
                stats.read_attempts += 1
                rs = time.perf_counter()
                size = store.get_into(key, ptr, args.value_size, read_cfg)
                re = time.perf_counter()
                latency_us = (re - rs) * 1e6
                if size >= 0:
                    stats.successful_reads += 1
                    stats.successful_read_time_us += latency_us
                    stats.read_latencies_us.append(latency_us)
                    done = True
                else:
                    stats.read_failures += 1
            else:
                buf.fill(ord("a") + ((thread_id + i + round_) % 26))
                stats.write_attempts += 1
                ws = time.perf_counter()
                rc = store.put_from(key, ptr, args.value_size, write_cfg)
                we = time.perf_counter()
                latency_us = (we - ws) * 1e6
                if rc == 0:
                    stats.successful_writes += 1
                    stats.successful_write_time_us += latency_us
                    stats.write_latencies_us.append(latency_us)
                else:
                    stats.write_failures += 1
                stats.exist_calls += 1
                exist_after = store.is_exist(key)
                if exist_after < 0:
                    stats.exist_failures += 1

    store.unregister_buffer(ptr)


def operation_sequence_workload(store: MooncakeDistributedStore, args: argparse.Namespace):
    remote_node_ids = [n for n in range(1, args.num_nodes + 1) if n != args.node_id]
    stats = [MixedThreadStats() for _ in range(args.num_threads)]
    threads = []
    for i in range(args.num_threads):
        th = threading.Thread(
            target=op_sequence_worker,
            args=(store, args, i, remote_node_ids, stats[i]),
        )
        threads.append(th)
        th.start()
    for th in threads:
        th.join()
    return print_mixed_results(stats, "Operation Sequence Workload", args.latency_dump_file)


def rw_correctness_worker(
    store: MooncakeDistributedStore,
    args: argparse.Namespace,
    thread_id: int,
    stats: MixedThreadStats,
):
    global _rw_mismatch_logged
    read_buf = np.zeros(args.value_size, dtype=np.uint8)
    write_buf = np.zeros(args.value_size, dtype=np.uint8)
    rptr = read_buf.ctypes.data
    wptr = write_buf.ctypes.data
    if store.register_buffer(rptr, args.value_size) != 0:
        stats.correctness_failures += 1
        return
    if store.register_buffer(wptr, args.value_size) != 0:
        stats.correctness_failures += 1
        store.unregister_buffer(rptr)
        return

    key_prefix = f"rwcheck_node_{args.node_id}_thread_{thread_id}"
    write_cfg = build_write_config(args.client_mode, strict_visibility=True)
    read_cfg = ReadRouteConfig()
    rng = random.Random(args.random_seed + 131 * thread_id)
    key_pool_size = max(1, args.rw_key_pool_size or args.key_count)

    expected = 0
    has_written = False
    write_version = 0
    current_key = ""

    for i in range(args.test_operation_nums):
        do_write = not has_written or (rng.random() < args.write_ratio)
        if do_write:
            key_idx = write_version % key_pool_size
            write_version += 1
            candidate_expected = (key_idx % 251) + 1
            candidate_key = f"{key_prefix}_k_{key_idx}"
            write_buf.fill(candidate_expected)
            stats.write_attempts += 1
            ws = time.perf_counter()
            rc = store.put_from(candidate_key, wptr, args.value_size, write_cfg)
            we = time.perf_counter()
            latency_us = (we - ws) * 1e6
            if rc == 0:
                has_written = True
                expected = candidate_expected
                current_key = candidate_key
                stats.successful_writes += 1
                stats.successful_write_time_us += latency_us
                stats.write_latencies_us.append(latency_us)
            else:
                stats.write_failures += 1
                continue

        if not has_written or not current_key:
            continue

        verified = False
        read_ok = False
        for retry in range(max(1, args.rw_verify_retries)):
            stats.read_attempts += 1
            read_buf.fill(0)
            rs = time.perf_counter()
            size = store.get_into(current_key, rptr, args.value_size, read_cfg)
            re = time.perf_counter()
            latency_us = (re - rs) * 1e6
            if size < 0 or size != args.value_size:
                stats.read_failures += 1
                if retry + 1 < args.rw_verify_retries:
                    time.sleep(args.rw_verify_sleep_ms / 1000.0)
                continue
            read_ok = True
            stats.successful_reads += 1
            stats.successful_read_time_us += latency_us
            stats.read_latencies_us.append(latency_us)

            mismatch = False
            mismatch_idx = -1
            actual_byte = 0
            for j in range(args.value_size):
                if int(read_buf[j]) != expected:
                    mismatch = True
                    mismatch_idx = j
                    actual_byte = int(read_buf[j])
                    break
            if not mismatch:
                verified = True
                break
            with _rw_mismatch_lock:
                if not _rw_mismatch_logged:
                    _rw_mismatch_logged = True
                    logger.error(
                        "rw_correctness mismatch (first): thread_id=%s key=%s expected=%s actual[%s]=%s",
                        thread_id,
                        current_key,
                        expected,
                        mismatch_idx,
                        actual_byte,
                    )
            if retry + 1 < args.rw_verify_retries:
                time.sleep(args.rw_verify_sleep_ms / 1000.0)

        if read_ok and not verified:
            stats.correctness_failures += 1

    store.unregister_buffer(wptr)
    store.unregister_buffer(rptr)


def concurrent_rw_correctness_workload(store: MooncakeDistributedStore, args: argparse.Namespace):
    stats = [MixedThreadStats() for _ in range(args.num_threads)]
    threads = []
    for i in range(args.num_threads):
        th = threading.Thread(
            target=rw_correctness_worker,
            args=(store, args, i, stats[i]),
        )
        threads.append(th)
        th.start()
    for th in threads:
        th.join()
    summary = print_mixed_results(stats, "Concurrent Read/Write Correctness", args.latency_dump_file)
    total_cf = sum(st.correctness_failures for st in stats)
    total_rf = sum(st.read_failures for st in stats)
    total_wf = sum(st.write_failures for st in stats)
    ok = total_cf == 0 and total_rf == 0 and total_wf == 0
    return summary, ok


def concurrent_write_worker(
    store: MooncakeDistributedStore,
    args: argparse.Namespace,
    thread_id: int,
    total_writes: int,
    write_start_idx: int,
    value_pattern: int,
    successful_keys: List[str],
    keys_lock: threading.Lock,
    stats: ConcurrentWriteThreadStats,
):
    buf = np.zeros(args.value_size, dtype=np.uint8)
    buf.fill(value_pattern)
    ptr = buf.ctypes.data
    if store.register_buffer(ptr, args.value_size) != 0:
        stats.write_failures += max(0, total_writes)
        return
    write_cfg = build_write_config(args.client_mode, strict_visibility=False)
    for i in range(total_writes):
        seq = write_start_idx + i
        key = f"cw_node_{args.node_id}_thread_{thread_id}_seq_{seq}"
        stats.write_attempts += 1
        ws = time.perf_counter()
        rc = store.put_from(key, ptr, args.value_size, write_cfg)
        we = time.perf_counter()
        latency_us = (we - ws) * 1e6
        if rc == 0:
            stats.successful_writes += 1
            stats.successful_write_time_us += latency_us
            stats.write_latencies_us.append(latency_us)
            with keys_lock:
                successful_keys.append(key)
        else:
            stats.write_failures += 1
    store.unregister_buffer(ptr)


def run_concurrent_write_phase(
    store: MooncakeDistributedStore, args: argparse.Namespace, num_threads: int, total_write_ops: int, title: str,
    latency_dump_file: str = "",
) -> ConcurrentWriteResult:
    result = ConcurrentWriteResult()
    if num_threads <= 0 or total_write_ops <= 0:
        logger.error("%s: invalid run config", title)
        return result

    result.thread_stats = [ConcurrentWriteThreadStats() for _ in range(num_threads)]
    keys_lock = threading.Lock()
    pattern = ord("W")
    base_ops = total_write_ops // num_threads
    remain = total_write_ops % num_threads
    next_start = 0
    threads = []
    for t in range(num_threads):
        ops = base_ops + (1 if t < remain else 0)
        start_idx = next_start
        next_start += ops
        th = threading.Thread(
            target=concurrent_write_worker,
            args=(
                store,
                args,
                t,
                ops,
                start_idx,
                pattern,
                result.successful_keys,
                keys_lock,
                result.thread_stats[t],
            ),
        )
        threads.append(th)
        th.start()
    for th in threads:
        th.join()

    all_lat: List[float] = []
    for st in result.thread_stats:
        result.write_attempts += st.write_attempts
        result.successful_writes += st.successful_writes
        result.write_failures += st.write_failures
        result.successful_write_time_us += st.successful_write_time_us
        all_lat.extend(st.write_latencies_us)

    result.p50, result.p80, result.p95, result.p99 = calculate_percentiles(all_lat)
    result.success_rate = (
        0.0
        if result.write_attempts == 0
        else result.successful_writes / result.write_attempts
    )
    result.write_ops_per_sec = (
        result.successful_writes * 1e6 / result.successful_write_time_us
        if result.successful_write_time_us > 0
        else 0.0
    )
    result.write_mb_per_sec = (
        (result.successful_writes * args.value_size)
        / ((result.successful_write_time_us / 1e6) * 1024 * 1024)
        if result.successful_write_time_us > 0
        else 0.0
    )

    logger.info("=== %s ===", title)
    logger.info(
        "threads=%s, write_attempts=%s, successful_writes=%s, write_failures=%s, success_rate(%%)=%s",
        num_threads,
        result.write_attempts,
        result.successful_writes,
        result.write_failures,
        result.success_rate * 100.0,
    )
    logger.info("Write latency(us) p50/p80/p95/p99 = %s/%s/%s/%s", result.p50, result.p80, result.p95, result.p99)

    if latency_dump_file:
        dump_latencies(latency_dump_file, write=all_lat)

    return result


def verify_concurrent_write_results(store: MooncakeDistributedStore, args: argparse.Namespace, keys: List[str]) -> bool:
    if not keys:
        logger.warning("No successful keys generated, skip verification")
        return False
    limit = len(keys) if args.cw_verify_sample_limit <= 0 else min(args.cw_verify_sample_limit, len(keys))
    read_buf = np.zeros(args.value_size, dtype=np.uint8)
    ptr = read_buf.ctypes.data
    if store.register_buffer(ptr, args.value_size) != 0:
        logger.error("Verification register_buffer failed")
        return False
    read_cfg = ReadRouteConfig()
    expected = ord("W")
    verify_passed = verify_failed = 0
    try:
        for i in range(limit):
            key = keys[i]
            read_buf.fill(0)
            size = store.get_into(key, ptr, args.value_size, read_cfg)
            ok = size == args.value_size
            if ok:
                ok = bool(np.all(read_buf == expected))
            if ok:
                verify_passed += 1
            else:
                verify_failed += 1
                if verify_failed <= 5:
                    logger.error("Verification failed for key=%s, size=%s", key, size)
    finally:
        store.unregister_buffer(ptr)
    logger.info(
        "=== Concurrent Write Verification === verify_total=%s verify_passed=%s verify_failed=%s",
        limit,
        verify_passed,
        verify_failed,
    )
    return verify_failed == 0


def concurrent_write_no_evict_workload(store: MooncakeDistributedStore, args: argparse.Namespace) -> Tuple[Dict[str, Any], bool]:
    if args.num_threads <= 0:
        logger.error("concurrent_write_no_evict: num_threads must be > 0")
        return {"workload": "concurrent_write_no_evict", "ok": False}, False
    if args.global_segment_size <= 0:
        logger.error("concurrent_write_no_evict: global_segment_size must be > 0")
        return {"workload": "concurrent_write_no_evict", "ok": False}, False
    if args.value_size <= 0:
        logger.error("concurrent_write_no_evict: value_size must be > 0")
        return {"workload": "concurrent_write_no_evict", "ok": False}, False

    total_segment_bytes = int(args.global_segment_size)
    target_bytes = int(math.ceil(total_segment_bytes * args.cw_target_fill_ratio))
    target_ops = max(1, (target_bytes + args.value_size - 1) // args.value_size)
    actual_payload_bytes = target_ops * args.value_size

    logger.info(
        "No-evict plan: num_threads=%s value_size=%s global_segment_size=%s fill_ratio=%s -> "
        "target_bytes=%s planned_key_count=%s actual_payload_bytes=%s",
        args.num_threads,
        args.value_size,
        total_segment_bytes,
        args.cw_target_fill_ratio,
        target_bytes,
        target_ops,
        actual_payload_bytes,
    )

    result = run_concurrent_write_phase(
        store,
        args,
        args.num_threads,
        target_ops,
        "Concurrent Write No-Evict",
        args.latency_dump_file,
    )
    perf_ok = result.successful_writes > 0
    vok = verify_concurrent_write_results(store, args, result.successful_keys)

    logger.info(
        "No-evict performance: write_ops_per_sec=%.2f write_mb_per_sec=%.2f success_rate=%.4f "
        "successful_writes=%s write_failures=%s latency_us p50/p80/p95/p99=%.0f/%.0f/%.0f/%.0f",
        result.write_ops_per_sec,
        result.write_mb_per_sec,
        result.success_rate,
        result.successful_writes,
        result.write_failures,
        result.p50,
        result.p80,
        result.p95,
        result.p99,
    )

    summary = {
        "workload": "concurrent_write_no_evict",
        "num_threads": args.num_threads,
        "value_size": args.value_size,
        "global_segment_size": total_segment_bytes,
        "cw_target_fill_ratio": args.cw_target_fill_ratio,
        "target_bytes": target_bytes,
        "planned_key_count": target_ops,
        "actual_payload_bytes": actual_payload_bytes,
        "successful_writes": result.successful_writes,
        "write_failures": result.write_failures,
        "success_rate": result.success_rate,
        "write_ops_per_sec": result.write_ops_per_sec,
        "write_mb_per_sec": result.write_mb_per_sec,
        "write_latency_p50_us": result.p50,
        "write_latency_p80_us": result.p80,
        "write_latency_p95_us": result.p95,
        "write_latency_p99_us": result.p99,
    }
    return summary, perf_ok and vok


def concurrent_write_with_evict_workload(store: MooncakeDistributedStore, args: argparse.Namespace) -> Tuple[Dict[str, Any], bool]:
    target_bytes = int(math.ceil(args.cw_base_memory_bytes * args.cw_evict_data_ratio))
    target_ops = max(1, (target_bytes + args.value_size - 1) // args.value_size)
    actual_payload_bytes = target_ops * args.value_size
    logger.info(
        "With-evict plan: num_threads=%s value_size=%s cw_base_memory_bytes=%s cw_evict_data_ratio=%s -> "
        "target_bytes=%s planned_key_count=%s actual_payload_bytes=%s",
        args.num_threads,
        args.value_size,
        args.cw_base_memory_bytes,
        args.cw_evict_data_ratio,
        target_bytes,
        target_ops,
        actual_payload_bytes,
    )
    result = run_concurrent_write_phase(
        store, args, args.num_threads, target_ops, "Concurrent Write With-Evict",
        args.latency_dump_file,
    )
    perf_ok = result.successful_writes > 0
    vok = verify_concurrent_write_results(store, args, result.successful_keys)

    logger.info(
        "With-evict performance: write_ops_per_sec=%.2f write_mb_per_sec=%.2f success_rate=%.4f "
        "successful_writes=%s write_failures=%s latency_us p50/p80/p95/p99=%.0f/%.0f/%.0f/%.0f",
        result.write_ops_per_sec,
        result.write_mb_per_sec,
        result.success_rate,
        result.successful_writes,
        result.write_failures,
        result.p50,
        result.p80,
        result.p95,
        result.p99,
    )

    summary = {
        "workload": "concurrent_write_with_evict",
        "num_threads": args.num_threads,
        "value_size": args.value_size,
        "cw_base_memory_bytes": args.cw_base_memory_bytes,
        "cw_evict_data_ratio": args.cw_evict_data_ratio,
        "target_bytes": target_bytes,
        "planned_key_count": target_ops,
        "actual_payload_bytes": actual_payload_bytes,
        "successful_writes": result.successful_writes,
        "write_failures": result.write_failures,
        "success_rate": result.success_rate,
        "write_ops_per_sec": result.write_ops_per_sec,
        "write_mb_per_sec": result.write_mb_per_sec,
        "write_latency_p50_us": result.p50,
        "write_latency_p80_us": result.p80,
        "write_latency_p95_us": result.p95,
        "write_latency_p99_us": result.p99,
    }
    return summary, perf_ok and vok


def wait_for_read_command(control_listen: str, read_event: threading.Event, shutdown: threading.Event):
    """Background thread: accept one line READ on control_listen host:port."""
    host, port_s = control_listen.rsplit(":", 1)
    port = int(port_s)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(1)
    srv.settimeout(1.0)
    logger.info("Control listener on %s:%s (send line READ to continue)", host, port)
    while not shutdown.is_set():
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        try:
            data = conn.recv(256).decode("utf-8", errors="ignore").strip().upper()
            if data.startswith("READ"):
                read_event.set()
            if data.startswith("EXIT"):
                shutdown.set()
        finally:
            conn.close()
    try:
        srv.close()
    except OSError:
        pass


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RealClient stress workload (Python)")
    p.add_argument("--protocol", default="tcp")
    p.add_argument("--master_address", default="127.0.0.1:50051")
    p.add_argument("--local_hostname", default="127.0.0.1")
    p.add_argument(
        "--metadata_connection_string",
        default="P2PHANDSHAKE",
        help="P2PHANDSHAKE or http://host:port/metadata",
    )
    p.add_argument("--device_names", default="", help="RDMA devices, comma-separated")
    p.add_argument(
        "--tiered_backend_config",
        default='{"tiers":[{"type":"DRAM","capacity":1073741824,"priority":10}]}',
    )
    p.add_argument("--client_rpc_port", type=int, default=12345)
    p.add_argument("--rpc_thread_num", type=int, default=16)
    p.add_argument("--node_id", type=int, default=1)
    p.add_argument("--num_nodes", type=int, default=1)
    p.add_argument("--key_count", type=int, default=1000)
    p.add_argument("--num_threads", type=int, default=8)
    p.add_argument("--test_operation_nums", type=int, default=1000)
    p.add_argument("--value_size", type=int, default=1048576)
    p.add_argument("--warmup_ops", type=int, default=100)
    p.add_argument("--remote_read_ratio", type=float, default=0.5)
    p.add_argument("--random_seed", type=int, default=12345)
    p.add_argument(
        "--start_timestamp_ms",
        type=int,
        default=0,
        help="Epoch ms for read phase alignment (once mode; orchestrated ignores)",
    )
    p.add_argument(
        "--workload_mode",
        default="preload_then_read",
        choices=[
            "preload_then_read",
            "op_sequence",
            "rw_correctness",
            "concurrent_write_no_evict",
            "concurrent_write_with_evict",
        ],
    )
    p.add_argument("--write_ratio", type=float, default=0.3)
    p.add_argument("--op_sequence_max_rounds", type=int, default=8)
    p.add_argument("--rw_verify_retries", type=int, default=50)
    p.add_argument("--rw_verify_sleep_ms", type=int, default=2)
    p.add_argument("--rw_key_pool_size", type=int, default=0)
    p.add_argument("--cw_max_threads", type=int, default=256)
    p.add_argument("--cw_success_rate_threshold", type=float, default=0.99)
    p.add_argument("--cw_thread_scale_base", type=int, default=2)
    p.add_argument(
        "--cw_target_fill_ratio",
        type=float,
        default=0.15,
        help="concurrent_write_no_evict: target data volume = global_segment_size * this ratio; "
        "key count = ceil(target_bytes / value_size)",
    )
    p.add_argument("--cw_evict_data_ratio", type=float, default=3.0)
    p.add_argument("--cw_base_memory_bytes", type=int, default=1073741824)
    p.add_argument("--cw_verify_sample_limit", type=int, default=0)
    p.add_argument("--client_mode", default="p2p", choices=["p2p", "centralized"])
    p.add_argument("--global_segment_size", type=int, default=2147483648)
    p.add_argument(
        "--local_buffer_size",
        type=int,
        default=16 * 1024 * 1024,
        help="Local buffer size bytes (P2P / centralized setup)",
    )
    p.add_argument("--post_test_wait_seconds", type=int, default=40)
    p.add_argument(
        "--run_mode",
        default="once",
        choices=["once", "orchestrated"],
        help="orchestrated: preload_then_read waits for READ on control port; then block forever at end",
    )
    p.add_argument(
        "--control_listen",
        default="0.0.0.0:0",
        help="Host:port to listen for READ (orchestrated preload_then_read). Port 0 = auto",
    )
    p.add_argument(
        "--latency_dump_file",
        default="",
        help="Path inside container to dump raw latency arrays as .npz (e.g. /tmp/node_1_latencies.npz)",
    )
    return p.parse_args()


def emit_stats_json(payload: Dict[str, Any], args: argparse.Namespace):
    payload = dict(payload)
    payload["node_id"] = args.node_id
    payload["workload_mode"] = args.workload_mode
    logger.info("STATS_JSON:%s", json.dumps(payload, separators=(",", ":")))


def main() -> int:
    args = parse_args()
    random.seed(args.random_seed)

    if args.remote_read_ratio < 0 or args.remote_read_ratio > 1:
        logger.error("remote_read_ratio must be in [0, 1]")
        return 1
    if args.write_ratio < 0 or args.write_ratio > 1:
        logger.error("write_ratio must be in [0, 1]")
        return 1
    if not (0 < args.cw_target_fill_ratio < 1):
        logger.error("cw_target_fill_ratio must be in (0, 1)")
        return 1
    if args.cw_evict_data_ratio <= 1.0:
        logger.error("cw_evict_data_ratio must be > 1")
        return 1
    if args.num_nodes <= 0 or args.node_id < 1 or args.node_id > args.num_nodes:
        logger.error("invalid node_id / num_nodes")
        return 1

    logger.info(
        "Starting RealClient stress workload (Python), node_id=%s master=%s local=%s",
        args.node_id,
        args.master_address,
        args.local_hostname,
    )

    store = initialize_store(args)
    ok = True
    stats_summary: Dict[str, Any] = {}

    read_event = threading.Event()
    shutdown = threading.Event()
    ctl_thread: Optional[threading.Thread] = None
    actual_control = args.control_listen

    try:
        if args.run_mode == "orchestrated" and args.workload_mode == "preload_then_read":
            host, port_s = args.control_listen.rsplit(":", 1)
            port = int(port_s)
            if port == 0:
                srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                srv.bind((host, 0))
                port = srv.getsockname()[1]
                srv.close()
                actual_control = f"{host}:{port}"
            logger.info("CONTROL_ENDPOINT:%s", actual_control)
            ctl_thread = threading.Thread(
                target=wait_for_read_command,
                args=(actual_control, read_event, shutdown),
                daemon=True,
            )
            ctl_thread.start()

        if args.workload_mode == "preload_then_read":
            ok = preload_keys(store, args)
            if ok:
                if args.run_mode == "orchestrated":
                    logger.info("Waiting for READ command on %s ...", actual_control)
                    while not read_event.is_set() and not shutdown.is_set():
                        time.sleep(0.1)
                    if shutdown.is_set():
                        store.close()
                        return 0
                elif args.start_timestamp_ms > 0:
                    now_ms = int(time.time() * 1000)
                    if args.start_timestamp_ms > now_ms:
                        w = (args.start_timestamp_ms - now_ms) / 1000.0
                        logger.info("preload done, sleeping %.3fs for start_timestamp_ms", w)
                        time.sleep(w)
                    else:
                        logger.warning("start_timestamp_ms in the past, starting read immediately")
                logger.info("preload_then_read: starting stress-read phase")
                stats_summary = stress_read(store, args)
        elif args.workload_mode == "op_sequence":
            stats_summary = operation_sequence_workload(store, args)
        elif args.workload_mode == "rw_correctness":
            stats_summary, ok = concurrent_rw_correctness_workload(store, args)
        elif args.workload_mode == "concurrent_write_no_evict":
            stats_summary, ok = concurrent_write_no_evict_workload(store, args)
        elif args.workload_mode == "concurrent_write_with_evict":
            stats_summary, ok = concurrent_write_with_evict_workload(store, args)
        else:
            ok = False

        if ok and stats_summary:
            emit_stats_json(stats_summary, args)

        logger.info("=== Test Completed ===")
        if args.post_test_wait_seconds > 0:
            logger.info(
                "Waiting %s seconds (post_test_wait_seconds)...",
                args.post_test_wait_seconds,
            )
            time.sleep(args.post_test_wait_seconds)

        if args.run_mode == "orchestrated":
            logger.info("Orchestrated mode: blocking forever; stop container to exit.")
            while True:
                time.sleep(3600)

        try:
            store.close()
        except Exception:
            pass
        return 0 if ok else 1
    except Exception:
        try:
            store.close()
        except Exception:
            pass
        raise


if __name__ == "__main__":
    sys.exit(main())
