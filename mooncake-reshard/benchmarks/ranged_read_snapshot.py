#!/usr/bin/env python3
import argparse
import json
import os
import socket
import statistics
import sys
import time

import numpy as np


def send_json(connection, value):
    connection.sendall((json.dumps(value) + "\n").encode())


def recv_json(connection):
    data = bytearray()
    while True:
        byte = connection.recv(1)
        if not byte:
            raise RuntimeError("coordination peer closed")
        if byte == b"\n":
            return json.loads(data)
        data.extend(byte)


def listen_one(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    server.listen(1)
    connection, _ = server.accept()
    server.close()
    return connection


def connect_to(host, port):
    while True:
        try:
            connection = socket.create_connection((host, port), timeout=5)
            connection.settimeout(None)
            return connection
        except OSError:
            time.sleep(0.2)


def import_store():
    runtime = os.environ["RUNTIME_PY"]
    if runtime not in sys.path:
        sys.path.insert(0, runtime)
    import store

    return store


def setup_store(module, args):
    client = module.MooncakeDistributedStore()
    result = client.setup(
        args.local_host,
        "P2PHANDSHAKE",
        args.global_segment_bytes,
        args.local_buffer_bytes,
        "rdma",
        args.rdma_device,
        args.master,
    )
    if result != 0:
        raise RuntimeError(f"Store setup failed: {result}")
    return client


def make_payload(index, nbytes):
    payload = np.arange(nbytes, dtype=np.uint8)
    payload += np.uint8((index * 17) & 0xFF)
    return payload


def put_objects(client, args):
    keys = [f"{args.key_prefix}:{index}" for index in range(args.objects)]
    payloads = [make_payload(index, args.object_bytes) for index in range(args.objects)]
    for key, payload in zip(keys, payloads):
        if client.register_buffer(payload.ctypes.data, payload.nbytes) != 0:
            raise RuntimeError(f"register writer buffer failed: {key}")
        result = client.put_from(key, payload.ctypes.data, payload.nbytes)
        if result != 0:
            raise RuntimeError(f"put_from failed for {key}: {result}")
    return keys, payloads


def build_workload(keys, payloads, args):
    if args.range_bytes > args.object_bytes:
        raise ValueError("range_bytes must not exceed object_bytes")
    output = np.empty(args.ranges * args.range_bytes, dtype=np.uint8)
    expected = np.empty_like(output)
    slots = args.object_bytes // args.range_bytes
    requests = []
    for index in range(args.ranges):
        object_index = (index * 17) % args.objects
        slot = (index * 1103515245 + object_index * 12345) % slots
        source_offset = slot * args.range_bytes
        target_offset = index * args.range_bytes
        expected[target_offset : target_offset + args.range_bytes] = payloads[
            object_index
        ][source_offset : source_offset + args.range_bytes]
        requests.append((object_index, source_offset, target_offset))

    batches = []
    for begin in range(0, len(requests), args.batch_ranges):
        grouped = {}
        for object_index, source_offset, target_offset in requests[
            begin : begin + args.batch_ranges
        ]:
            group = grouped.setdefault(object_index, ([], [], []))
            group[0].append(target_offset)
            group[1].append(source_offset)
            group[2].append(args.range_bytes)
        object_indices = list(grouped)
        batches.append(
            (
                [output.ctypes.data],
                [[keys[index] for index in object_indices]],
                [[grouped[index][0] for index in object_indices]],
                [[grouped[index][1] for index in object_indices]],
                [[grouped[index][2] for index in object_indices]],
            )
        )
    return output, expected, batches


def validate_results(results, sizes):
    if len(results) != 1 or len(results[0]) != len(sizes[0]):
        raise RuntimeError("ranged-read result shape mismatch")
    for actual_group, expected_group in zip(results[0], sizes[0]):
        if list(actual_group) != list(expected_group):
            raise RuntimeError("ranged-read returned a partial result")


def percentile(samples, fraction):
    ordered = sorted(samples)
    return ordered[round((len(ordered) - 1) * fraction)]


def benchmark(client, keys, payloads, args, location):
    output, expected, batches = build_workload(keys, payloads, args)
    if client.register_buffer(output.ctypes.data, output.nbytes) != 0:
        raise RuntimeError("register destination buffer failed")

    def run_once(mode):
        start = time.perf_counter_ns()
        snapshot = None
        if mode == "snapshot":
            snapshot = client.prepare_get_into_ranges_snapshot(keys)
        for addresses, all_keys, dst_offsets, src_offsets, sizes in batches:
            if snapshot is None:
                results = client.get_into_ranges(
                    addresses, all_keys, dst_offsets, src_offsets, sizes
                )
            else:
                results = client.get_into_ranges_from_snapshot(
                    snapshot, addresses, all_keys, dst_offsets, src_offsets, sizes
                )
            validate_results(results, sizes)
        elapsed_us = (time.perf_counter_ns() - start) / 1000
        if not np.array_equal(output, expected):
            raise RuntimeError(f"payload mismatch in {mode} mode")
        return elapsed_us

    try:
        for _ in range(args.warmup):
            run_once("plain")
            run_once("snapshot")
        samples = {"plain": [], "snapshot": []}
        for iteration in range(args.iters):
            order = (
                ("plain", "snapshot")
                if iteration % 2 == 0
                else (
                    "snapshot",
                    "plain",
                )
            )
            for mode in order:
                samples[mode].append(run_once(mode))
    finally:
        client.unregister_buffer(output.ctypes.data)

    summaries = {}
    for mode, values in samples.items():
        p50 = statistics.median(values)
        p95 = percentile(values, 0.95)
        summaries[mode] = {
            "p50_us": p50,
            "p95_us": p95,
            "useful_gbps_p50": output.nbytes * 8 / p50 / 1000,
        }
    result = {
        "location": location,
        "objects": args.objects,
        "object_bytes": args.object_bytes,
        "ranges": args.ranges,
        "range_bytes": args.range_bytes,
        "batch_ranges": args.batch_ranges,
        "batches": len(batches),
        "payload_bytes": output.nbytes,
        "iters": args.iters,
        "plain": summaries["plain"],
        "snapshot": summaries["snapshot"],
        "speedup": summaries["plain"]["p50_us"] / summaries["snapshot"]["p50_us"],
    }
    print("RESHARD_RANGE_SUMMARY " + json.dumps(result, sort_keys=True), flush=True)
    return result


def writer(module, args):
    connection = listen_one(args.coord_port)
    client = setup_store(module, args)
    keys, payloads = put_objects(client, args)
    send_json(connection, keys)
    if recv_json(connection) != "done":
        raise RuntimeError("bad reader acknowledgement")
    for payload in payloads:
        client.unregister_buffer(payload.ctypes.data)
    client.close()


def reader(module, args):
    connection = connect_to(args.writer_host, args.coord_port)
    client = setup_store(module, args)
    keys = recv_json(connection)
    payloads = [make_payload(index, args.object_bytes) for index in range(args.objects)]
    benchmark(client, keys, payloads, args, "remote")
    send_json(connection, "done")
    client.close()


def local(module, args):
    client = setup_store(module, args)
    keys, payloads = put_objects(client, args)
    benchmark(client, keys, payloads, args, "local")
    for payload in payloads:
        client.unregister_buffer(payload.ctypes.data)
    client.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("writer", "reader", "local"))
    parser.add_argument("--local-host", required=True)
    parser.add_argument("--master", required=True)
    parser.add_argument("--writer-host", default="127.0.0.1")
    parser.add_argument("--coord-port", type=int, default=56117)
    parser.add_argument("--rdma-device", default="erdma_0")
    parser.add_argument("--key-prefix", default="reshard-snapshot-bench")
    parser.add_argument("--objects", type=int, default=24)
    parser.add_argument("--object-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--ranges", type=int, default=98304)
    parser.add_argument("--range-bytes", type=int, default=264)
    parser.add_argument("--batch-ranges", type=int, default=1024)
    parser.add_argument("--iters", type=int, default=9)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--global-segment-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--local-buffer-bytes", type=int, default=256 * 1024 * 1024)
    args = parser.parse_args()
    if (
        min(
            args.objects,
            args.object_bytes,
            args.ranges,
            args.range_bytes,
            args.batch_ranges,
            args.iters,
        )
        <= 0
    ):
        parser.error("workload dimensions must be positive")
    return args


def main():
    args = parse_args()
    module = import_store()
    if args.role == "writer":
        writer(module, args)
    elif args.role == "reader":
        reader(module, args)
    else:
        local(module, args)


if __name__ == "__main__":
    main()
