#!/usr/bin/env python3
"""Benchmark local named byte tables.

Run: python scripts/bench_engram_store_27b.py --local-dir /dev/shm/engram-bench
"""

import argparse
import time
import uuid

import numpy as np

from mooncake.engram import EngramStore

BATCH_SIZES = [1, 4, 16, 64, 128, 256]
NUM_WARMUP = 10
NUM_ITER = 50
TABLE_VOCAB_SIZES = [50000] * 8
ROW_BYTES = 64


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-dir", default="/dev/shm/engram-bench")
    args = parser.parse_args()
    specs = {
        f"head/{head}": {"num_rows": count, "row_bytes": ROW_BYTES}
        for head, count in enumerate(TABLE_VOCAB_SIZES)
    }
    names = list(specs)
    with EngramStore(
        tables=specs,
        namespace=f"benchmark/{uuid.uuid4().hex}",
        local_dir=args.local_dir,
        persistent=False,
    ) as store:
        rng = np.random.default_rng(42)
        rows = [
            rng.integers(0, 256, (count, ROW_BYTES), dtype=np.uint8)
            for count in TABLE_VOCAB_SIZES
        ]
        for name, table in zip(names, rows):
            store.populate(name, table)
        print("Batch   Mean(ms)    P50(ms)    P99(ms)       Tok/s       GB/s")
        for batch in BATCH_SIZES:
            ids = np.column_stack(
                [rng.integers(count, size=batch) for count in TABLE_VOCAB_SIZES]
            )
            out = np.empty((batch, len(names), ROW_BYTES), np.uint8)
            store.lookup(names, ids, out)
            for head, table in enumerate(rows):
                np.testing.assert_array_equal(out[:, head], table[ids[:, head]])
            for _ in range(NUM_WARMUP):
                store.lookup(names, ids, out)
            times = []
            for _ in range(NUM_ITER):
                start = time.perf_counter()
                store.lookup(names, ids, out)
                times.append(time.perf_counter() - start)
            mean = np.mean(times)
            p50, p99 = np.percentile(times, [50, 99])
            print(
                f"{batch:5d} {mean * 1e3:10.3f} {p50 * 1e3:10.3f} "
                f"{p99 * 1e3:10.3f} {batch / mean:11.1f} "
                f"{out.nbytes / mean / 1e9:10.3f}"
            )


if __name__ == "__main__":
    main()
