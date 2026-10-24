#!/usr/bin/env python3
"""Benchmark named byte tables locally or against an existing Mooncake Store.

Local:  python scripts/bench_engram_store_27b.py --local-dir /dev/shm/engram-bench
Remote: configure MooncakeConfig environment variables, then run without flags.
"""

import argparse
import time
import uuid
from contextlib import contextmanager

import numpy as np

from mooncake.engram import EngramStore

BATCH_SIZES = [1, 4, 16, 64, 128, 256]
NUM_WARMUP = 10
NUM_ITER = 50
TABLE_VOCAB_SIZES = [50000] * 8
ROW_BYTES = 64


@contextmanager
def registered_output(client, shape):
    output = np.empty(shape, np.uint8)
    if client is not None and client.register_buffer(output.ctypes.data, output.nbytes):
        raise RuntimeError("Could not register benchmark output")
    try:
        yield output
    finally:
        if client is not None and client.unregister_buffer(output.ctypes.data):
            raise RuntimeError("Could not unregister benchmark output")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-dir")
    args = parser.parse_args()
    client = None
    if args.local_dir is None:
        from mooncake.mooncake_config import MooncakeConfig
        from mooncake.store import MooncakeDistributedStore

        config = MooncakeConfig.load_from_env()
        client = MooncakeDistributedStore()
        rc = client.setup(
            config.local_hostname,
            config.metadata_server,
            config.global_segment_size,
            config.local_buffer_size,
            config.protocol,
            config.device_name,
            config.master_server_address,
        )
        if rc:
            client.close()
            raise RuntimeError(f"Failed to setup Mooncake Store: {rc}")

    specs = {
        f"head/{head}": {"num_rows": count, "row_bytes": ROW_BYTES}
        for head, count in enumerate(TABLE_VOCAB_SIZES)
    }
    names = list(specs)
    published = []
    try:
        with EngramStore(
            tables=specs,
            namespace=f"benchmark/{uuid.uuid4().hex}",
            local_dir=args.local_dir,
            store_client=client,
            persistent=client is not None,
        ) as store:
            try:
                rng = np.random.default_rng(42)
                rows = [
                    rng.integers(0, 256, (count, ROW_BYTES), dtype=np.uint8)
                    for count in TABLE_VOCAB_SIZES
                ]
                for name, table in zip(names, rows):
                    store.populate(name, table)
                    published.append(name)
                print("Batch   Mean(ms)    P50(ms)    P99(ms)       Tok/s       GB/s")
                for batch in BATCH_SIZES:
                    ids = np.column_stack(
                        [rng.integers(count, size=batch) for count in TABLE_VOCAB_SIZES]
                    )
                    with registered_output(
                        client, (batch, len(names), ROW_BYTES)
                    ) as out:
                        store.lookup(names, ids, out)
                        for head, table in enumerate(rows):
                            np.testing.assert_array_equal(
                                out[:, head], table[ids[:, head]]
                            )
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
            finally:
                if client is not None:
                    for name in published:
                        store.remove(name)
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
