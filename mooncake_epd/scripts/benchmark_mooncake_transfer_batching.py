#!/usr/bin/env python3
"""Benchmark real Mooncake batch submission on a local peer.

This benchmark does not mock the transfer engine.  It creates two real
``mooncake.engine.TransferEngine`` instances, allocates registered managed
buffers, and transfers the same payload while varying descriptors per submit.
It is intended to calibrate connector-side transport chunking without loading
an LLM or conflating transfer costs with model execution.
"""

from __future__ import annotations

import argparse
import gc
import importlib.metadata
import json
import math
import os
import platform
import socket
import statistics
import time
from pathlib import Path
from typing import Any, Iterable


def _parse_positive_ints(raw: str) -> list[int]:
    values = sorted({int(value.strip()) for value in str(raw).split(",") if value.strip()})
    if not values or any(value <= 0 for value in values):
        raise ValueError("batch sizes must be a non-empty list of positive integers")
    return values


def _percentile(values: Iterable[float], percentile: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def _groups(total_descriptors: int, descriptors_per_submit: int) -> list[slice]:
    return [
        slice(start, min(total_descriptors, start + descriptors_per_submit))
        for start in range(0, total_descriptors, descriptors_per_submit)
    ]


def _transfer_once(
    engine: Any,
    remote_session: str,
    src_ptrs: list[int],
    dst_ptrs: list[int],
    lengths: list[int],
    descriptors_per_submit: int,
) -> float:
    started = time.perf_counter()
    for group in _groups(len(src_ptrs), descriptors_per_submit):
        ret_code = engine.batch_transfer_sync_write(
            remote_session,
            src_ptrs[group],
            dst_ptrs[group],
            lengths[group],
        )
        if int(ret_code) != 0:
            raise RuntimeError(
                "Mooncake batch transfer failed: "
                f"ret={ret_code}, descriptors={group.stop - group.start}"
            )
    return (time.perf_counter() - started) * 1000.0


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    protocol = str(args.protocol).strip().lower()
    if protocol == "tcp" and args.force_tcp:
        # Must be set before importing the extension.  Otherwise topology auto
        # discovery can install RDMA even when initialize(..., "tcp", ...) was
        # requested, which invalidates a TCP-only comparison.
        os.environ["MC_FORCE_TCP"] = "1"

    from mooncake.engine import TransferEngine

    total_descriptors = int(args.descriptors)
    bytes_per_descriptor = int(args.bytes_per_descriptor)
    total_bytes = total_descriptors * bytes_per_descriptor
    batch_sizes = [
        min(total_descriptors, value)
        for value in _parse_positive_ints(args.batch_sizes)
    ]
    batch_sizes = sorted(set(batch_sizes))

    source = TransferEngine()
    target = TransferEngine()
    source_ptr = 0
    target_ptr = 0
    try:
        source_ret = source.initialize(
            str(args.host),
            "P2PHANDSHAKE",
            protocol,
            str(args.device_name),
        )
        target_ret = target.initialize(
            str(args.host),
            "P2PHANDSHAKE",
            protocol,
            str(args.device_name),
        )
        if int(source_ret) != 0 or int(target_ret) != 0:
            raise RuntimeError(
                f"Mooncake initialization failed: source={source_ret}, target={target_ret}"
            )

        source_ptr = int(source.allocate_managed_buffer(total_bytes))
        target_ptr = int(target.allocate_managed_buffer(total_bytes))
        if source_ptr == 0 or target_ptr == 0:
            raise MemoryError(f"failed to allocate two managed buffers of {total_bytes} bytes")

        pattern = bytes((index % 251 for index in range(total_bytes)))
        source.write_bytes_to_buffer(source_ptr, pattern, total_bytes)
        target.write_bytes_to_buffer(target_ptr, b"\x00" * total_bytes, total_bytes)

        src_ptrs = [
            source_ptr + index * bytes_per_descriptor
            for index in range(total_descriptors)
        ]
        dst_ptrs = [
            target_ptr + index * bytes_per_descriptor
            for index in range(total_descriptors)
        ]
        lengths = [bytes_per_descriptor] * total_descriptors
        remote_session = f"{args.host}:{target.get_rpc_port()}"

        results: list[dict[str, Any]] = []
        for batch_size in batch_sizes:
            for _ in range(int(args.warmup)):
                _transfer_once(
                    source,
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                    batch_size,
                )
            samples_ms = [
                _transfer_once(
                    source,
                    remote_session,
                    src_ptrs,
                    dst_ptrs,
                    lengths,
                    batch_size,
                )
                for _ in range(int(args.repeats))
            ]
            median_ms = statistics.median(samples_ms)
            results.append(
                {
                    "descriptors_per_submit": batch_size,
                    "submits_per_payload": math.ceil(total_descriptors / batch_size),
                    "samples": len(samples_ms),
                    "elapsed_ms_avg": statistics.fmean(samples_ms),
                    "elapsed_ms_p50": median_ms,
                    "elapsed_ms_p95": _percentile(samples_ms, 0.95),
                    "elapsed_ms_min": min(samples_ms),
                    "elapsed_ms_max": max(samples_ms),
                    "effective_gbps_p50": (
                        total_bytes * 8.0 / (median_ms * 1_000_000.0)
                        if median_ms > 0.0
                        else None
                    ),
                }
            )

        head = target.read_bytes_from_buffer(target_ptr, min(4096, total_bytes))
        tail_size = min(4096, total_bytes)
        tail = target.read_bytes_from_buffer(
            target_ptr + total_bytes - tail_size,
            tail_size,
        )
        correctness_ok = (
            head == pattern[: len(head)]
            and tail == pattern[-len(tail) :]
        )
        if not correctness_ok:
            raise AssertionError("destination buffer differs from source pattern")

        best = min(results, key=lambda item: float(item["elapsed_ms_p50"]))
        return {
            "schema_version": 1,
            "benchmark": "real_mooncake_transfer_batching",
            "created_at": time.time(),
            "environment": {
                "hostname": socket.gethostname(),
                "platform": platform.platform(),
                "python": platform.python_version(),
                "mooncake_transfer_engine": importlib.metadata.version(
                    "mooncake-transfer-engine"
                ),
                "protocol": protocol,
                "force_tcp": bool(args.force_tcp),
                "device_name": str(args.device_name),
                "source_rpc_port": int(source.get_rpc_port()),
                "target_rpc_port": int(target.get_rpc_port()),
            },
            "workload": {
                "descriptors": total_descriptors,
                "bytes_per_descriptor": bytes_per_descriptor,
                "total_bytes": total_bytes,
                "warmup": int(args.warmup),
                "repeats": int(args.repeats),
                "batch_sizes": batch_sizes,
            },
            "correctness_ok": correctness_ok,
            "results": results,
            "best_p50": best,
        }
    finally:
        if source_ptr:
            source.free_managed_buffer(source_ptr, total_bytes)
        if target_ptr:
            target.free_managed_buffer(target_ptr, total_bytes)
        del source
        del target
        gc.collect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--protocol", choices=("tcp", "rdma"), default="tcp")
    parser.add_argument("--device-name", default="")
    parser.add_argument("--force-tcp", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--descriptors", type=int, default=128)
    parser.add_argument("--bytes-per-descriptor", type=int, default=32 * 1024)
    parser.add_argument("--batch-sizes", default="1,2,4,8,16,32,64,128")
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.descriptors <= 0 or args.bytes_per_descriptor <= 0:
        parser.error("descriptors and bytes-per-descriptor must be positive")
    if args.warmup < 0 or args.repeats <= 0:
        parser.error("warmup must be non-negative and repeats must be positive")
    return args


def main() -> int:
    args = parse_args()
    payload = run_benchmark(args)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
