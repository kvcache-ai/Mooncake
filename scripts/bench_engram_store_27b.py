#!/usr/bin/env python3
"""Benchmark local named byte tables.

Run: python scripts/bench_engram_store_27b.py --local-dir /dev/shm/engram-bench
"""

import argparse
import time
import tracemalloc
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
    parser.add_argument(
        "--contiguous",
        action="store_true",
        help="Use consecutive row IDs to measure the direct-copy path.",
    )
    parser.add_argument(
        "--large-output",
        action="store_true",
        help="Use two 2048-byte-wide tables and batch sizes 256 and 16384.",
    )
    parser.add_argument(
        "--head-major",
        action="store_true",
        help="Allocate table-major output and transpose it to token-major.",
    )
    args = parser.parse_args()
    if args.large_output and args.contiguous:
        parser.error("--large-output cannot be combined with --contiguous")
    batch_sizes = [256, 16384] if args.large_output else BATCH_SIZES
    vocab_sizes = [4096] * 2 if args.large_output else TABLE_VOCAB_SIZES
    row_bytes = 2048 if args.large_output else ROW_BYTES
    specs = {
        f"head/{head}": {"num_rows": count, "row_bytes": row_bytes}
        for head, count in enumerate(vocab_sizes)
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
            rng.integers(0, 256, (count, row_bytes), dtype=np.uint8)
            for count in vocab_sizes
        ]
        for name, table in zip(names, rows):
            store.populate(name, table)
        print(
            "Batch   Mean(ms)    P50(ms)    P99(ms)       Tok/s       GB/s   Peak(MiB)"
        )
        for batch in batch_sizes:
            if args.contiguous:
                ids = np.column_stack(
                    [
                        np.arange(start, start + batch)
                        for count in vocab_sizes
                        for start in [int(rng.integers(0, count - batch))]
                    ]
                )
            else:
                ids = np.column_stack(
                    [rng.integers(count, size=batch) for count in vocab_sizes]
                )
            out = np.empty(
                (len(names), batch, row_bytes)
                if args.head_major
                else (batch, len(names), row_bytes),
                np.uint8,
            )
            if args.head_major:
                out = out.transpose(1, 0, 2)
            store.lookup(names, ids, out)
            for head, table in enumerate(rows):
                np.testing.assert_array_equal(out[:, head], table[ids[:, head]])
            for _ in range(NUM_WARMUP):
                store.lookup(names, ids, out)
            tracemalloc.start()
            store.lookup(names, ids, out)
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
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
                f"{out.nbytes / mean / 1e9:10.3f} {peak / 2**20:11.3f}"
            )


if __name__ == "__main__":
    main()
