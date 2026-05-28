#!/usr/bin/env python3
"""RL weight transfer verification with TP mismatch scenarios.

Validates that get_into_ranges correctly handles scatter-gather reads
when Trainer and Rollouter have different Tensor Parallelism (TP) configurations.

Scenarios:
  - TP 4→8 (split): each Rollouter rank reads half of one Trainer shard
  - TP 8→4 (merge): each Rollouter rank concatenates two full Trainer shards

Requires a running mooncake-store-client daemon (dummy mode).

Example:
    python3 scripts/tests/client/test_tp_weight_transfer.py \
      --daemon_addr 127.0.0.1:16590
"""

from __future__ import annotations

import argparse
import ctypes
import sys

from mooncake.store import MooncakeDistributedStore, MooncakeHostMemAllocator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--daemon_addr",
        "--daemon-addr",
        required=True,
        help="Dummy server address, e.g. 127.0.0.1:16590",
    )
    parser.add_argument(
        "--tenant",
        default="default",
        help="Tenant for operations (default: default)",
    )
    parser.add_argument(
        "--keyspace",
        default=None,
        help="Optional keyspace/worker scope",
    )
    return parser.parse_args()


def make_shard_data(shard_id: int, shard_size: int) -> bytes:
    return bytes((shard_id + i) & 0xFF for i in range(shard_size))


def run_tp_split(store: MooncakeDistributedStore, allocator: MooncakeHostMemAllocator, tenant: str):
    """TP 4→8: Trainer produces 4 shards (256B each), Rollouter has 8 ranks (128B each)."""
    print("  [TP 4→8 split] ", end="", flush=True)

    trainer_tp = 4
    rollouter_tp = 8
    shard_size = 256
    rank_size = (shard_size * trainer_tp) // rollouter_tp  # 128

    # Trainer writes shards
    for sid in range(trainer_tp):
        key = f"tp_split.layer.0.weight.tp{sid}"
        data = make_shard_data(sid, shard_size)
        store.put(key, data, tenant=tenant)

    # Allocate Rollouter buffers
    buffer_ptrs = []
    for _ in range(rollouter_tp):
        ptr = allocator.alloc(rank_size)
        store.register_buffer(ptr, rank_size)
        ctypes.memset(ptr, 0xFF, rank_size)
        buffer_ptrs.append(ptr)

    # Build scatter-read parameters
    all_keys = []
    all_dst_offsets = []
    all_src_offsets = []
    all_sizes = []

    for rank in range(rollouter_tp):
        trainer_shard = rank // 2
        src_offset = (rank % 2) * rank_size
        key = f"tp_split.layer.0.weight.tp{trainer_shard}"
        all_keys.append([key])
        all_dst_offsets.append([[0]])
        all_src_offsets.append([[src_offset]])
        all_sizes.append([[rank_size]])

    results = store.get_into_ranges(
        buffer_ptrs,
        all_keys,
        all_dst_offsets,
        all_src_offsets,
        all_sizes,
        buffer_sizes=[rank_size] * rollouter_tp,
        tenant=tenant,
    )

    try:
        # Verify results
        for rank, buf_results in enumerate(results):
            for key_idx, key_results in enumerate(buf_results):
                for frag_idx, val in enumerate(key_results):
                    assert val > 0, f"rank {rank} key {key_idx} frag {frag_idx} failed: {val}"

        # Verify buffer contents
        for rank in range(rollouter_tp):
            trainer_shard = rank // 2
            src_offset = (rank % 2) * rank_size
            expected = make_shard_data(trainer_shard, shard_size)[src_offset:src_offset + rank_size]
            actual = (ctypes.c_ubyte * rank_size).from_address(buffer_ptrs[rank])
            assert bytes(actual) == expected, f"rank {rank} content mismatch"
    finally:
        for ptr in buffer_ptrs:
            store.unregister_buffer(ptr, rank_size)

    print("PASS")


def run_tp_merge(store: MooncakeDistributedStore, allocator: MooncakeHostMemAllocator, tenant: str):
    """TP 8→4: Trainer produces 8 shards (128B each), Rollouter has 4 ranks (256B each)."""
    print("  [TP 8→4 merge] ", end="", flush=True)

    trainer_tp = 8
    rollouter_tp = 4
    shard_size = 128
    rank_size = (shard_size * trainer_tp) // rollouter_tp  # 256

    # Trainer writes shards
    for sid in range(trainer_tp):
        key = f"tp_merge.layer.0.weight.tp{sid}"
        data = make_shard_data(sid, shard_size)
        store.put(key, data, tenant=tenant)

    # Allocate Rollouter buffers
    buffer_ptrs = []
    for _ in range(rollouter_tp):
        ptr = allocator.alloc(rank_size)
        store.register_buffer(ptr, rank_size)
        ctypes.memset(ptr, 0xFF, rank_size)
        buffer_ptrs.append(ptr)

    # Build scatter-read parameters
    all_keys = []
    all_dst_offsets = []
    all_src_offsets = []
    all_sizes = []

    for rank in range(rollouter_tp):
        shard_a = 2 * rank
        shard_b = 2 * rank + 1
        key_a = f"tp_merge.layer.0.weight.tp{shard_a}"
        key_b = f"tp_merge.layer.0.weight.tp{shard_b}"

        all_keys.append([key_a, key_b])
        all_dst_offsets.append([[0], [shard_size]])
        all_src_offsets.append([[0], [0]])
        all_sizes.append([[shard_size], [shard_size]])

    results = store.get_into_ranges(
        buffer_ptrs,
        all_keys,
        all_dst_offsets,
        all_src_offsets,
        all_sizes,
        buffer_sizes=[rank_size] * rollouter_tp,
        tenant=tenant,
    )

    try:
        # Verify results
        for rank, buf_results in enumerate(results):
            for key_idx, key_results in enumerate(buf_results):
                for frag_idx, val in enumerate(key_results):
                    assert val > 0, f"rank {rank} key {key_idx} frag {frag_idx} failed: {val}"

        # Verify buffer contents
        for rank in range(rollouter_tp):
            shard_a = 2 * rank
            shard_b = 2 * rank + 1
            expected_a = make_shard_data(shard_a, shard_size)
            expected_b = make_shard_data(shard_b, shard_size)
            actual = (ctypes.c_ubyte * rank_size).from_address(buffer_ptrs[rank])
            actual_bytes = bytes(actual)
            assert actual_bytes[:shard_size] == expected_a, f"rank {rank} first half mismatch"
            assert actual_bytes[shard_size:] == expected_b, f"rank {rank} second half mismatch"
    finally:
        for ptr in buffer_ptrs:
            store.unregister_buffer(ptr, rank_size)

    print("PASS")


def main():
    args = parse_args()

    store = MooncakeDistributedStore()
    store.setup_dummy(
        64 * 1024 * 1024,
        16 * 1024 * 1024,
        args.daemon_addr,
        keyspace=args.keyspace,
    )
    allocator = MooncakeHostMemAllocator()

    print("RL weight transfer TP mismatch tests:")
    run_tp_split(store, allocator, args.tenant)
    run_tp_merge(store, allocator, args.tenant)
    print("\nAll tests PASSED.")

    store.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        sys.exit(1)
