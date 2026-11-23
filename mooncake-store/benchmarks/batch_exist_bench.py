#!/usr/bin/env python3
"""End-to-end Python benchmark for batch_is_exist serial vs parallel.

Unlike batch_exist_bench (C++, server-only), this measures the full
pybind -> RealClient -> MasterClient -> RPC -> master path, using an
embedded master so no external mooncake_master process is required.

Usage:
    python batch_exist_bench.py \
        --num-objects 100000 --key-sizes 1000,10000,100000 \
        --hit-ratio 0.5 --repetitions 10
"""

from __future__ import annotations

import argparse
import os
import statistics
import time

from mooncake.store import ExistOptions, MooncakeDistributedStore

SEGMENT_SIZE = 64 * 1024 * 1024
LOCAL_BUFFER_SIZE = 32 * 1024 * 1024


def setup_embedded_master(store: MooncakeDistributedStore) -> int:
    return store.setup(
        "localhost",
        "P2PHANDSHAKE",
        SEGMENT_SIZE,
        LOCAL_BUFFER_SIZE,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        "",
        enable_embedded_master=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-objects", type=int, default=100000)
    parser.add_argument("--key-sizes", default="1000,10000,100000")
    parser.add_argument("--hit-ratio", type=float, default=0.5)
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=3)
    args = parser.parse_args()

    store = MooncakeDistributedStore()
    ret = setup_embedded_master(store)
    if ret != 0:
        raise RuntimeError(f"embedded-master setup failed: {ret}")

    sizes = [int(x) for x in args.key_sizes.split(",")]

    print(f"# prefilling {args.num_objects} objects ...")
    for i in range(args.num_objects):
        store.put(f"bench_exist_key_{i}", b"x")

    opts = ExistOptions()
    opts.chunked_parallel = True

    print(
        "num_keys,hit_ratio,serial_p50_us,parallel_p50_us,speedup,"
        "serial_keys_per_s,parallel_keys_per_s"
    )
    for n in sizes:
        if n > args.num_objects:
            print(f"# skip {n}: > num-objects {args.num_objects}")
            continue

        keys = []
        hit_mod = int(args.hit_ratio * 100)
        for i in range(n):
            if i % 100 < hit_mod:
                keys.append(f"bench_exist_key_{i % args.num_objects}")
            else:
                keys.append(f"bench_exist_missing_{i}")

        for _ in range(args.warmup):
            store.batch_is_exist(keys)
            store.batch_is_exist(keys, opts)

        serial_us = []
        parallel_us = []
        for _ in range(args.repetitions):
            t0 = time.perf_counter()
            store.batch_is_exist(keys)
            t1 = time.perf_counter()
            store.batch_is_exist(keys, opts)
            t2 = time.perf_counter()
            serial_us.append((t1 - t0) * 1e6)
            parallel_us.append((t2 - t1) * 1e6)

        s_p50 = statistics.median(serial_us)
        p_p50 = statistics.median(parallel_us)
        speedup = s_p50 / p_p50 if p_p50 > 0 else 0.0
        print(
            f"{n},{args.hit_ratio},{s_p50:.1f},{p_p50:.1f},{speedup:.2f},"
            f"{n * 1e6 / s_p50:.0f},{n * 1e6 / p_p50:.0f}"
        )

    store.close()


if __name__ == "__main__":
    main()
