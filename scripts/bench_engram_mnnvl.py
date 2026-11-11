#!/usr/bin/env python3
"""Real-GPU Engram byte checks, side-stream/Graph replay, and microbenchmark;
failures never downgrade or SKIP.
"""

import argparse
import json
import multiprocessing
import os
import tempfile
import time

import numpy as np


def pattern(rows, width, head):
    rows = np.asarray(rows, dtype=np.uint64)[..., None]
    columns = np.arange(width, dtype=np.uint64)
    # High row bits feed the pattern so offsets wrapping past 2 GiB cannot
    # coincidentally produce equal bytes.
    return (
        (
            rows * 131
            + (rows >> 8) * 17
            + (rows >> 16) * 29
            + (rows >> 24) * 43
            + columns
            + head * 37
        )
        & 255
    ).astype(np.uint8)


def make_rows(count, width, head):
    rows = np.empty((count, width), dtype=np.uint8)
    chunk = max(1, (4 << 20) // width)
    for start in range(0, count, chunk):
        end = min(count, start + chunk)
        rows[start:end] = pattern(np.arange(start, end, dtype=np.uint64), width, head)
    return rows


def worker(caller, device, args):
    import torch
    from mooncake.engram import EngramStore

    torch.cuda.set_device(device)
    names = [f"head/{head}" for head in range(args.heads)]
    specs = {
        name: {"num_rows": args.rows, "row_bytes": args.row_bytes} for name in names
    }
    owners = {name: head % args.nodes for head, name in enumerate(names)}
    with tempfile.TemporaryDirectory(prefix="mooncake-engram-") as directory:
        store = EngramStore(
            tables=specs,
            namespace="engram-microbenchmark-v1",
            local_dir=directory,
            persistent=False,
        )
        started = time.perf_counter()
        graph = None
        try:
            if args.backend == "mnnvl":
                rows = {
                    name: make_rows(args.rows, args.row_bytes, head)
                    for head, name in enumerate(names)
                    if caller == 0 and owners[name] == args.node_id
                }
                store.populate_shard_and_sync(
                    rows,
                    backend="mnnvl",
                    shard_id=args.node_id,
                    num_shards=args.nodes,
                    table_owners=owners,
                    master_addr=args.master_addr,
                    session_id=args.session_id,
                    caller_id=caller,
                    num_callers=len(args.devices),
                    max_lookup_rows=args.tokens * args.heads,
                    max_egm_bytes_per_numa=args.budget_bytes,
                    device=device,
                    host_id=args.host_id,
                    rpc_hostname=args.rpc_hostname,
                    init_timeout_s=args.init_timeout,
                    close_timeout_s=args.close_timeout,
                )
                del rows
            else:
                for head, name in enumerate(names):
                    store.populate(name, make_rows(args.rows, args.row_bytes, head))
            initialized_s = time.perf_counter() - started
            offsets = [head * (args.rows + 1) for head in range(args.heads)]
            ids = torch.empty(
                (args.tokens, args.heads), dtype=torch.int64, device=device
            )
            shape = (
                (args.heads, args.tokens, args.row_bytes)
                if args.head_major
                else (args.tokens, args.heads, args.row_bytes)
            )
            output = torch.empty(shape, dtype=torch.uint8, device=device)
            if args.head_major:
                output = output.permute(1, 0, 2)
            prefetch = torch.cuda.Stream(device=device)
            prefetch.wait_stream(torch.cuda.current_stream(device))
            graph = None
            with torch.cuda.stream(prefetch):
                ids.fill_(-1)
                store.lookup(
                    names, ids, output, offsets=offsets, stream=prefetch.cuda_stream
                )
                prefetch.synchronize()
                if args.graph:
                    graph = torch.cuda.CUDAGraph()
                    with torch.cuda.graph(graph, stream=prefetch):
                        store.lookup(
                            names,
                            ids,
                            output,
                            offsets=offsets,
                            stream=prefetch.cuda_stream,
                        )
                for iteration in range(3):
                    rng = np.random.default_rng(100 + iteration)
                    local_ids = rng.integers(
                        0, args.rows, size=(args.tokens, args.heads), dtype=np.int64
                    )
                    local_ids[0, :] = args.rows - 1
                    cpu_ids = local_ids + np.array(offsets, dtype=np.int64)
                    cpu_ids[-1, :] = -1
                    ids.copy_(torch.from_numpy(cpu_ids))
                    output.fill_(221)
                    if graph:
                        graph.replay()
                    else:
                        store.lookup(
                            names,
                            ids,
                            output,
                            offsets=offsets,
                            stream=prefetch.cuda_stream,
                        )
                    prefetch.synchronize()
                    expected = np.stack(
                        [
                            pattern(local_ids[:, head], args.row_bytes, head)
                            for head in range(args.heads)
                        ],
                        axis=1,
                    )
                    expected[-1, :] = 221
                    np.testing.assert_array_equal(output.cpu().numpy(), expected)
                begin, end = (
                    torch.cuda.Event(enable_timing=True),
                    torch.cuda.Event(enable_timing=True),
                )
                begin.record(prefetch)
                for _ in range(args.iterations):
                    if graph:
                        graph.replay()
                    else:
                        store.lookup(
                            names,
                            ids,
                            output,
                            offsets=offsets,
                            stream=prefetch.cuda_stream,
                        )
                end.record(prefetch)
                end.synchronize()
                milliseconds = begin.elapsed_time(end) / args.iterations
            torch.cuda.current_stream(device).wait_stream(prefetch)
            prefetch.synchronize()
            graph = None
            report = {
                "status": "passed",
                "backend": args.backend,
                "node": args.node_id,
                "caller": caller,
                "device": device,
                "graph": args.graph,
                "head_major": args.head_major,
                "byte_checks": 3,
                "init_seconds": initialized_s,
                "lookup_ms": milliseconds,
                "logical_lookup_bytes": (args.tokens - 1) * args.heads * args.row_bytes,
                "table_bytes": args.rows * args.heads * args.row_bytes,
                "cross_node": args.backend == "mnnvl" and args.nodes > 1,
            }
        finally:
            # Exceptions are reported too; a close timeout retains provider
            # resources not yet confirmed reclaimable.
            torch.cuda.synchronize(device)
            graph = None
            store.close()
        print(json.dumps(report), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("local", "mnnvl"), default="mnnvl")
    parser.add_argument("--master-addr", default="http://127.0.0.1:8080/metadata")
    parser.add_argument(
        "--session-id", required=True, help="same on all nodes; change every run"
    )
    parser.add_argument("--nodes", type=int, default=2)
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument(
        "--devices", default="0", help="visible CUDA ordinals, e.g. 0,1,2,3"
    )
    parser.add_argument(
        "--host-id",
        help="unique identity of the actual physical node in containers",
    )
    parser.add_argument(
        "--rpc-hostname", default="", help="routable local IP of this node"
    )
    parser.add_argument("--heads", type=int, default=4)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--row-bytes", type=int, default=264)
    parser.add_argument("--tokens", type=int, default=128)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--budget-bytes", type=int, default=8 << 30)
    parser.add_argument("--init-timeout", type=float, default=300)
    parser.add_argument("--close-timeout", type=float, default=60)
    parser.add_argument("--graph", action="store_true")
    parser.add_argument("--head-major", action="store_true")
    args = parser.parse_args()
    args.devices = [int(item) for item in args.devices.split(",")]
    if (
        not args.devices
        or min(args.devices) < 0
        or len(set(args.devices)) != len(args.devices)
        or min(args.nodes, args.heads, args.rows, args.row_bytes, args.iterations) < 1
        or args.tokens < 2
        or not 0 <= args.node_id < args.nodes
        or args.heads < args.nodes
    ):
        parser.error(
            "requires unique devices, positive sizes, at least two tokens, "
            "heads>=nodes, and a valid node-id"
        )
    if args.backend == "mnnvl" and not os.environ.get("MC_ENABLE_MNNVL"):
        parser.error(
            "set MC_ENABLE_MNNVL=1 before launch; will not fall back to "
            "another transport"
        )
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(target=worker, args=(caller, device, args))
        for caller, device in enumerate(args.devices)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    return 0 if all(process.exitcode == 0 for process in processes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
