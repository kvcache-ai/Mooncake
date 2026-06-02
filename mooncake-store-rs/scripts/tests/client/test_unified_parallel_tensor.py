#!/usr/bin/env python3
"""E2E tests for the Unified Parallel Tensor IO API.

Validates the full write/read cycle for parallelism-aware tensor operations,
including cross-TP scatter-gather reconstruction, writer partitions, and
multi-axis layouts.

Requires a running mooncake-store-client daemon (dummy mode) and torch.

Example:
    python3 scripts/tests/client/test_unified_parallel_tensor.py \
      --daemon_addr 127.0.0.1:16590
"""

from __future__ import annotations

import argparse
import ctypes
import sys
import struct

import torch

from mooncake.store import (
    MooncakeDistributedStore,
    MooncakeHostMemAllocator,
    ParallelAxis,
    TensorParallelism,
    ReadTarget,
    AXIS_DP,
    AXIS_TP,
    AXIS_EP,
    AXIS_PP,
    READ_MODE_AS_STORED,
    READ_MODE_SHARD,
    READ_MODE_FULL,
    TENSOR_METADATA_WIRE_SIZE,
)


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


def DP(rank: int, size: int) -> ParallelAxis:
    return ParallelAxis(AXIS_DP, rank, size)


def TP(rank: int, size: int, split_dim: int = 0) -> ParallelAxis:
    return ParallelAxis(AXIS_TP, rank, size, split_dim=split_dim)


def PP(rank: int, size: int) -> ParallelAxis:
    return ParallelAxis(AXIS_PP, rank, size)


def generate_weight(shape, dtype=torch.float32, seed=42) -> torch.Tensor:
    gen = torch.Generator().manual_seed(seed)
    return torch.randn(shape, dtype=dtype, generator=gen)


def compute_tp_shard(weight: torch.Tensor, rank: int, tp_size: int, split_dim: int = 0) -> torch.Tensor:
    total = weight.shape[split_dim]
    chunk = total // tp_size
    return weight.narrow(split_dim, rank * chunk, chunk).contiguous()


def assert_tensor_equal(actual: torch.Tensor, expected: torch.Tensor, context: str = ""):
    if not torch.equal(actual, expected):
        diff = (actual - expected).abs()
        raise AssertionError(
            f"tensor mismatch{' (' + context + ')' if context else ''}: "
            f"max_diff={diff.max().item()}, actual_shape={list(actual.shape)}, "
            f"expected_shape={list(expected.shape)}"
        )


# ─── Test cases ─────────────────────────────────────────────────────────────

passed = 0
failed = 0
skipped = 0


class SkipTest(Exception):
    pass


def run_test(name, fn, *args, **kwargs):
    global passed, failed, skipped
    print(f"  [{name}] ", end="", flush=True)
    try:
        fn(*args, **kwargs)
        print("PASS")
        passed += 1
    except SkipTest as e:
        print(f"SKIP: {e}")
        skipped += 1
    except Exception as e:
        print(f"FAIL: {e}")
        failed += 1


def test_as_stored_roundtrip(store, tenant):
    """put_tensor_with_parallelism(parallelism=None) -> get(target=AS_STORED)"""
    weight = generate_weight([64, 128], seed=1)
    store.put_tensor_with_parallelism("test_as_stored.weight", weight, tenant=tenant)
    result = store.get_tensor_with_parallelism(
        "test_as_stored.weight",
        target=ReadTarget(READ_MODE_AS_STORED),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight, "as_stored roundtrip")


def test_full_tensor_roundtrip(store, tenant):
    """put full tensor -> get(target=FULL)"""
    weight = generate_weight([32, 64], seed=2)
    store.put_tensor_with_parallelism("test_full_rt.weight", weight, tenant=tenant)
    result = store.get_tensor_with_parallelism(
        "test_full_rt.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight, "full roundtrip")


def test_tp_shard_exact_match(store, tenant):
    """Write TP=4 shards (auto-sliced from full), read back with exact same TP=4."""
    full_weight = generate_weight([256, 128], seed=3)
    tp_size = 4

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_tp_exact.weight", full_weight, parallelism=par, tenant=tenant
        )

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        result = store.get_tensor_with_parallelism(
            "test_tp_exact.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, tp_size)
        assert_tensor_equal(result, expected, f"tp exact rank {rank}")


def test_tp_shard_legacy_compat(store, tenant):
    """Verify single-TP key format is _tp_N (legacy compatible)."""
    weight = generate_weight([64, 32], seed=4)
    par = TensorParallelism([TP(2, 4)])
    store.put_tensor_with_parallelism(
        "test_legacy.weight", weight, parallelism=par, tenant=tenant
    )

    raw = store.get("test_legacy.weight_tp_2", tenant=tenant)
    assert len(raw) > TENSOR_METADATA_WIRE_SIZE, "legacy key should exist"

    magic = struct.unpack_from("<I", raw, 0)[0]
    assert magic == 0x4D4F4F4E, f"bad magic in legacy key: {magic:#x}"


def test_tp_split_4to8(store, tenant):
    """TP 4->8 split: Trainer TP=4, Rollouter TP=8."""
    full_weight = generate_weight([256, 128], seed=5)
    trainer_tp = 4
    rollouter_tp = 8

    for rank in range(trainer_tp):
        par = TensorParallelism([TP(rank, trainer_tp)])
        store.put_tensor_with_parallelism(
            "test_split.weight", full_weight, parallelism=par, tenant=tenant
        )

    for rank in range(rollouter_tp):
        par = TensorParallelism([TP(rank, rollouter_tp)])
        result = store.get_tensor_with_parallelism(
            "test_split.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, rollouter_tp)
        assert_tensor_equal(result, expected, f"split 4->8 rank {rank}")


def test_tp_merge_8to4(store, tenant):
    """TP 8->4 merge: Trainer TP=8, Rollouter TP=4."""
    full_weight = generate_weight([256, 128], seed=6)
    trainer_tp = 8
    rollouter_tp = 4

    for rank in range(trainer_tp):
        par = TensorParallelism([TP(rank, trainer_tp)])
        store.put_tensor_with_parallelism(
            "test_merge.weight", full_weight, parallelism=par, tenant=tenant
        )

    for rank in range(rollouter_tp):
        par = TensorParallelism([TP(rank, rollouter_tp)])
        result = store.get_tensor_with_parallelism(
            "test_merge.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, rollouter_tp)
        assert_tensor_equal(result, expected, f"merge 8->4 rank {rank}")


def test_full_reconstruction(store, tenant):
    """Write TP=4 shards (auto-sliced), reconstruct full tensor with ReadTarget(FULL)."""
    full_weight = generate_weight([128, 64], seed=7)
    tp_size = 4

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_full_recon.weight", full_weight, parallelism=par, tenant=tenant
        )

    result = store.get_tensor_with_parallelism(
        "test_full_recon.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "full reconstruction")


def test_dp_tp_shard_roundtrip(store, tenant):
    """Write DP=2 TP=4 shards (auto-sliced from full), read back with exact match.

    Note: all DP ranks store the same TP shard data here because this test
    validates multi-axis key naming and retrieval, not DP data semantics.
    """
    full_weight = generate_weight([128, 64], seed=8)
    dp_size = 2
    tp_size = 4

    for dp_rank in range(dp_size):
        for tp_rank in range(tp_size):
            par = TensorParallelism([DP(dp_rank, dp_size), TP(tp_rank, tp_size)])
            store.put_tensor_with_parallelism(
                "test_dp_tp.weight", full_weight, parallelism=par, tenant=tenant
            )

    for dp_rank in range(dp_size):
        for tp_rank in range(tp_size):
            par = TensorParallelism([DP(dp_rank, dp_size), TP(tp_rank, tp_size)])
            result = store.get_tensor_with_parallelism(
                "test_dp_tp.weight",
                target=ReadTarget(READ_MODE_SHARD, parallelism=par),
                tenant=tenant,
            )
            expected = compute_tp_shard(full_weight, tp_rank, tp_size)
            assert_tensor_equal(result, expected, f"dp_tp dp={dp_rank} tp={tp_rank}")


def test_dp_tp_key_format(store, tenant):
    """Verify multi-axis key format: key__dp_0of2__tp_1of4_sd0."""
    weight = generate_weight([32, 16], seed=9)
    par = TensorParallelism([DP(0, 2), TP(1, 4)])
    store.put_tensor_with_parallelism(
        "test_key_fmt.weight", weight, parallelism=par, tenant=tenant
    )

    raw = store.get("test_key_fmt.weight__dp_0of2__tp_1of4_sd0", tenant=tenant)
    assert len(raw) > TENSOR_METADATA_WIRE_SIZE, "multi-axis key should exist"


def test_upsert_overwrites(store, tenant):
    """Upsert should overwrite existing data (auto-sliced from full)."""
    weight_v1 = generate_weight([32, 16], seed=10)
    weight_v2 = generate_weight([32, 16], seed=11)
    par = TensorParallelism([TP(0, 2)])

    store.put_tensor_with_parallelism(
        "test_upsert.weight", weight_v1, parallelism=par, tenant=tenant
    )
    store.upsert_tensor_with_parallelism(
        "test_upsert.weight", weight_v2, parallelism=par, tenant=tenant
    )

    result = store.get_tensor_with_parallelism(
        "test_upsert.weight",
        target=ReadTarget(READ_MODE_SHARD, parallelism=par),
        tenant=tenant,
    )
    expected = compute_tp_shard(weight_v2, 0, 2)
    assert_tensor_equal(result, expected, "upsert should be v2 shard")


def test_shard_read_into_buffer(store, allocator, tenant):
    """Read a shard into a pre-registered buffer."""
    full_weight = generate_weight([64, 32], seed=12)
    tp_size = 2

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_into_buf.weight", full_weight, parallelism=par, tenant=tenant
        )

    shard_0 = compute_tp_shard(full_weight, 0, tp_size)
    buf_size = TENSOR_METADATA_WIRE_SIZE + shard_0.nelement() * shard_0.element_size()
    ptr = allocator.alloc(buf_size)
    result = store.register_buffer(ptr, buf_size)
    if result != 0:
        raise SkipTest(f"register_buffer not supported (result={result})")


    try:
        par = TensorParallelism([TP(0, tp_size)])
        read_result = store.get_tensor_with_parallelism_into(
            "test_into_buf.weight",
            ptr,
            buf_size,
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        assert read_result.data_bytes == shard_0.nelement() * shard_0.element_size()
        actual_data = (ctypes.c_ubyte * read_result.data_bytes).from_address(read_result.data_ptr)
        expected_ptr = shard_0.data_ptr()
        expected_data = (ctypes.c_ubyte * read_result.data_bytes).from_address(expected_ptr)
        assert bytes(actual_data) == bytes(expected_data), "buffer content mismatch"
    finally:
        store.unregister_buffer(ptr, buf_size)


def test_invalid_axis_rejected(store, tenant):
    """Invalid axis spec (rank >= size) should raise."""
    weight = generate_weight([32, 16], seed=13)
    try:
        par = TensorParallelism([TP(4, 4)])  # rank=4, size=4 -> invalid
        store.put_tensor_with_parallelism(
            "test_invalid.weight", weight, parallelism=par, tenant=tenant
        )
        raise AssertionError("should have raised for invalid axis")
    except ValueError:
        pass


def test_read_nonexistent_shard(store, tenant):
    """Reading a non-existent shard should fail gracefully."""
    par = TensorParallelism([TP(0, 4)])
    try:
        store.get_tensor_with_parallelism(
            "nonexistent_key_12345.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        raise AssertionError("should have raised for non-existent key")
    except RuntimeError:
        pass


def test_rl_weight_sync_e2e(store, tenant):
    """Mock RL weight sync: Trainer (TP=4) -> Rollouter (TP=8) -> Critic (TP=2)."""
    full_weight = generate_weight([512, 256], seed=42)
    trainer_tp = 4

    for tp_rank in range(trainer_tp):
        par = TensorParallelism([TP(tp_rank, trainer_tp)])
        store.put_tensor_with_parallelism(
            "rl_sync.layers.0.weight", full_weight, parallelism=par, tenant=tenant
        )

    # Rollouter: TP=8 reads (split)
    rollouter_tp = 8
    for rank in range(rollouter_tp):
        par = TensorParallelism([TP(rank, rollouter_tp)])
        result = store.get_tensor_with_parallelism(
            "rl_sync.layers.0.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, rollouter_tp)
        assert_tensor_equal(result, expected, f"rollouter rank {rank}")

    # Critic: TP=2 reads (merge)
    critic_tp = 2
    for rank in range(critic_tp):
        par = TensorParallelism([TP(rank, critic_tp)])
        result = store.get_tensor_with_parallelism(
            "rl_sync.layers.0.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, critic_tp)
        assert_tensor_equal(result, expected, f"critic rank {rank}")

    # Full reconstruction
    result = store.get_tensor_with_parallelism(
        "rl_sync.layers.0.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "rl_sync full reconstruction")


def test_rl_weight_sync_multi_layer(store, tenant):
    """Multi-layer RL weight sync: 3 layers, different shapes."""
    shapes = [[256, 128], [128, 64], [64, 32]]
    trainer_tp = 4
    rollouter_tp = 8

    for layer_idx, shape in enumerate(shapes):
        weight = generate_weight(shape, seed=100 + layer_idx)
        for tp_rank in range(trainer_tp):
            par = TensorParallelism([TP(tp_rank, trainer_tp)])
            store.put_tensor_with_parallelism(
                f"multi_layer.layers.{layer_idx}.weight",
                weight,
                parallelism=par,
                tenant=tenant,
            )

    for layer_idx, shape in enumerate(shapes):
        weight = generate_weight(shape, seed=100 + layer_idx)
        for rank in range(rollouter_tp):
            par = TensorParallelism([TP(rank, rollouter_tp)])
            result = store.get_tensor_with_parallelism(
                f"multi_layer.layers.{layer_idx}.weight",
                target=ReadTarget(READ_MODE_SHARD, parallelism=par),
                tenant=tenant,
            )
            expected = compute_tp_shard(weight, rank, rollouter_tp)
            assert_tensor_equal(
                result, expected, f"multi_layer L{layer_idx} rank {rank}"
            )


# ─── New test cases: coverage gaps ────────────────────────────────────────


def test_tp_split_dim1(store, tenant):
    """TP split along dim=1 instead of default dim=0."""
    full_weight = generate_weight([64, 128], seed=20)
    tp_size = 4

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size, split_dim=1)])
        store.put_tensor_with_parallelism(
            "test_sd1.weight", full_weight, parallelism=par, tenant=tenant
        )

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size, split_dim=1)])
        result = store.get_tensor_with_parallelism(
            "test_sd1.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, tp_size, split_dim=1)
        assert_tensor_equal(result, expected, f"split_dim1 rank {rank}")


def test_dtype_bfloat16(store, tenant):
    """bfloat16 tensor round-trip through parallelism API (auto-sliced)."""
    gen = torch.Generator().manual_seed(21)
    weight = torch.randn([64, 32], dtype=torch.bfloat16, generator=gen)
    par = TensorParallelism([TP(0, 2)])
    store.put_tensor_with_parallelism(
        "test_bf16.weight", weight, parallelism=par, tenant=tenant
    )
    result = store.get_tensor_with_parallelism(
        "test_bf16.weight",
        target=ReadTarget(READ_MODE_SHARD, parallelism=par),
        tenant=tenant,
    )
    expected = compute_tp_shard(weight, 0, 2)
    assert_tensor_equal(result, expected, "bfloat16 roundtrip")


def test_dtype_float16(store, tenant):
    """float16 tensor round-trip through parallelism API."""
    gen = torch.Generator().manual_seed(22)
    weight = torch.randn([64, 32], dtype=torch.float16, generator=gen)
    store.put_tensor_with_parallelism(
        "test_fp16.weight", weight, tenant=tenant
    )
    result = store.get_tensor_with_parallelism(
        "test_fp16.weight",
        target=ReadTarget(READ_MODE_AS_STORED),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight, "float16 roundtrip")


def test_1d_tensor_tp(store, tenant):
    """1D tensor [1024] with TP=4 sharding (auto-sliced)."""
    full_weight = generate_weight([1024], seed=23)
    tp_size = 4

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_1d.weight", full_weight, parallelism=par, tenant=tenant
        )

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        result = store.get_tensor_with_parallelism(
            "test_1d.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, tp_size)
        assert result.shape == expected.shape, f"1d shape mismatch: {result.shape} vs {expected.shape}"
        assert_tensor_equal(result, expected, f"1d rank {rank}")

    result = store.get_tensor_with_parallelism(
        "test_1d.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "1d full reconstruction")


def test_tp_size_1(store, tenant):
    """TP=1 degenerate case: single shard = full tensor."""
    weight = generate_weight([32, 16], seed=24)
    par = TensorParallelism([TP(0, 1)])
    store.put_tensor_with_parallelism(
        "test_tp1.weight", weight, parallelism=par, tenant=tenant
    )
    result = store.get_tensor_with_parallelism(
        "test_tp1.weight",
        target=ReadTarget(READ_MODE_SHARD, parallelism=par),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight, "tp=1 shard")

    result = store.get_tensor_with_parallelism(
        "test_tp1.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight, "tp=1 full")


def test_pp_only_shard(store, tenant):
    """PP-only axis (no TP): write and read back."""
    weight = generate_weight([32, 16], seed=25)
    par = TensorParallelism([PP(0, 2)])
    store.put_tensor_with_parallelism(
        "test_pp.weight", weight, parallelism=par, tenant=tenant
    )

    raw = store.get("test_pp.weight__pp_0of2", tenant=tenant)
    assert len(raw) > TENSOR_METADATA_WIRE_SIZE, "pp key should exist"

    par_read = TensorParallelism([PP(0, 2)])
    result = store.get_tensor_with_parallelism(
        "test_pp.weight",
        target=ReadTarget(READ_MODE_SHARD, parallelism=par_read),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight, "pp only roundtrip")


def test_read_into_preallocated_tensor(store, tenant):
    """Read into a pre-allocated tensor via tensor= parameter."""
    weight = generate_weight([64, 32], seed=26)
    store.put_tensor_with_parallelism(
        "test_prealloc.weight", weight, tenant=tenant
    )
    target_tensor = torch.empty_like(weight)
    result = store.get_tensor_with_parallelism(
        "test_prealloc.weight",
        target=ReadTarget(READ_MODE_AS_STORED),
        tensor=target_tensor,
        tenant=tenant,
    )
    assert_tensor_equal(target_tensor, weight, "preallocated tensor content")


def test_tp_split_2to16(store, tenant):
    """TP 2→16: large fan-out, each reader gets 1/8 of a source shard."""
    full_weight = generate_weight([256, 128], seed=27)
    writer_tp = 2
    reader_tp = 16

    for rank in range(writer_tp):
        par = TensorParallelism([TP(rank, writer_tp)])
        store.put_tensor_with_parallelism(
            "test_2to16.weight", full_weight, parallelism=par, tenant=tenant
        )

    for rank in range(reader_tp):
        par = TensorParallelism([TP(rank, reader_tp)])
        result = store.get_tensor_with_parallelism(
            "test_2to16.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, reader_tp)
        assert_tensor_equal(result, expected, f"2to16 rank {rank}")


def test_upsert_direct_full(store, tenant):
    """Upsert without parallelism (DirectFull route)."""
    weight_v1 = generate_weight([32, 16], seed=28)
    weight_v2 = generate_weight([32, 16], seed=29)

    store.put_tensor_with_parallelism(
        "test_upsert_full.weight", weight_v1, tenant=tenant
    )
    store.upsert_tensor_with_parallelism(
        "test_upsert_full.weight", weight_v2, tenant=tenant
    )

    result = store.get_tensor_with_parallelism(
        "test_upsert_full.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight_v2, "upsert direct full should be v2")


def test_tp_split_dim1_cross_tp(store, tenant):
    """Cross-TP reconstruction with split_dim=1: writer TP=2, reader TP=4."""
    full_weight = generate_weight([64, 128], seed=30)
    writer_tp = 2
    reader_tp = 4

    for rank in range(writer_tp):
        par = TensorParallelism([TP(rank, writer_tp, split_dim=1)])
        store.put_tensor_with_parallelism(
            "test_sd1_cross.weight", full_weight, parallelism=par, tenant=tenant
        )

    for rank in range(reader_tp):
        par = TensorParallelism([TP(rank, reader_tp, split_dim=1)])
        result = store.get_tensor_with_parallelism(
            "test_sd1_cross.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, reader_tp, split_dim=1)
        assert_tensor_equal(result, expected, f"sd1 cross 2->4 rank {rank}")


def test_tp_split_dim1_merge_cross_tp(store, tenant):
    """Cross-TP merge with split_dim=1: writer TP=4, reader TP=2."""
    full_weight = generate_weight([64, 128], seed=31)
    writer_tp = 4
    reader_tp = 2

    for rank in range(writer_tp):
        par = TensorParallelism([TP(rank, writer_tp, split_dim=1)])
        store.put_tensor_with_parallelism(
            "test_sd1_merge.weight", full_weight, parallelism=par, tenant=tenant
        )

    for rank in range(reader_tp):
        par = TensorParallelism([TP(rank, reader_tp, split_dim=1)])
        result = store.get_tensor_with_parallelism(
            "test_sd1_merge.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, reader_tp, split_dim=1)
        assert_tensor_equal(result, expected, f"sd1 merge 4->2 rank {rank}")


def test_tp_split_dim1_full_reconstruction(store, tenant):
    """Full reconstruction from split_dim=1 shards."""
    full_weight = generate_weight([64, 128], seed=32)
    tp_size = 4

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size, split_dim=1)])
        store.put_tensor_with_parallelism(
            "test_sd1_full.weight", full_weight, parallelism=par, tenant=tenant
        )

    result = store.get_tensor_with_parallelism(
        "test_sd1_full.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "sd1 full reconstruction")


def test_writer_partition_roundtrip(store, tenant):
    """Writer partition: 4 writers each put full tensor (auto-sliced), reader gets FULL."""
    full_weight = generate_weight([256, 128], seed=40)
    num_writers = 4
    split_dim = 0

    for rank in range(num_writers):
        store.put_tensor_with_parallelism(
            "test_wp_rt.weight",
            full_weight,
            writer_partition=(rank, num_writers, split_dim),
            tenant=tenant,
        )

    result = store.get_tensor_with_parallelism(
        "test_wp_rt.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "writer partition full roundtrip")


def test_writer_partition_to_tp_read(store, tenant):
    """Writer partition=4 stored, reader reads with TP=2 (shard mode)."""
    full_weight = generate_weight([128, 64], seed=41)
    num_writers = 4
    split_dim = 0

    for rank in range(num_writers):
        store.put_tensor_with_parallelism(
            "test_wp_tp.weight",
            full_weight,
            writer_partition=(rank, num_writers, split_dim),
            tenant=tenant,
        )

    reader_tp = 2
    for rank in range(reader_tp):
        par = TensorParallelism([TP(rank, reader_tp)])
        result = store.get_tensor_with_parallelism(
            "test_wp_tp.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, reader_tp, split_dim=split_dim)
        assert_tensor_equal(result, expected, f"wp->tp rank {rank}")


def test_writer_partition_split_dim1(store, tenant):
    """Writer partition with split_dim=1."""
    full_weight = generate_weight([64, 128], seed=42)
    num_writers = 4
    split_dim = 1

    for rank in range(num_writers):
        store.put_tensor_with_parallelism(
            "test_wp_sd1.weight",
            full_weight,
            writer_partition=(rank, num_writers, split_dim),
            tenant=tenant,
        )

    result = store.get_tensor_with_parallelism(
        "test_wp_sd1.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "writer partition sd1 full roundtrip")


def test_writer_partition_upsert(store, tenant):
    """Upsert correctness for writer_partition (auto-sliced)."""
    weight_v1 = generate_weight([64, 32], seed=43)
    weight_v2 = generate_weight([64, 32], seed=44)
    num_writers = 2
    split_dim = 0

    for rank in range(num_writers):
        store.put_tensor_with_parallelism(
            "test_wp_upsert.weight",
            weight_v1,
            writer_partition=(rank, num_writers, split_dim),
            tenant=tenant,
        )

    for rank in range(num_writers):
        store.upsert_tensor_with_parallelism(
            "test_wp_upsert.weight",
            weight_v2,
            writer_partition=(rank, num_writers, split_dim),
            tenant=tenant,
        )

    result = store.get_tensor_with_parallelism(
        "test_wp_upsert.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight_v2, "writer partition upsert should be v2")


# ─── New test cases: batch, _from, manifest, writer key ─────────────────


def build_raw_buffer(tensor: torch.Tensor) -> tuple:
    """Build a raw [TensorMetadata | data] buffer from a tensor.

    Returns (buffer_ptr, total_size, alloc_ref) where alloc_ref keeps
    the underlying memory alive.
    """
    data_ptr = tensor.data_ptr()
    data_bytes = tensor.nelement() * tensor.element_size()
    ndim = tensor.dim()
    shape = list(tensor.shape)

    dtype_map = {
        torch.float32: 0, torch.float64: 1, torch.int8: 2, torch.uint8: 3,
        torch.int16: 4, torch.float16: 11, torch.bfloat16: 12,
        torch.int32: 6, torch.int64: 8,
    }
    dtype_i32 = dtype_map[tensor.dtype]

    header = struct.pack("<IHHII", 0x4D4F4F4E, 1, 0, dtype_i32, ndim)
    header += struct.pack("<IIqq", 0, 0, TENSOR_METADATA_WIRE_SIZE, data_bytes)

    global_shape = b""
    for i in range(8):
        global_shape += struct.pack("<q", shape[i] if i < ndim else 0)

    local_shape = b""
    for i in range(8):
        local_shape += struct.pack("<q", shape[i] if i < ndim else 0)

    axes = b"\x00" * 128
    axis_count = struct.pack("<I", 0)
    padding = b"\x00" * (TENSOR_METADATA_WIRE_SIZE - len(header) - len(global_shape) - len(local_shape) - len(axes) - len(axis_count))

    metadata_bytes = header + global_shape + local_shape + axes + axis_count + padding
    assert len(metadata_bytes) == TENSOR_METADATA_WIRE_SIZE

    total_size = TENSOR_METADATA_WIRE_SIZE + data_bytes
    buf = (ctypes.c_ubyte * total_size)()
    ctypes.memmove(buf, metadata_bytes, TENSOR_METADATA_WIRE_SIZE)
    ctypes.memmove(
        ctypes.addressof(buf) + TENSOR_METADATA_WIRE_SIZE,
        data_ptr,
        data_bytes,
    )
    return ctypes.addressof(buf), total_size, buf


def test_batch_put_get_tp(store, tenant):
    """Batch put full tensors (auto-sliced) + batch get shards back."""
    full_weight = generate_weight([128, 64], seed=50)
    tp_size = 4
    keys = ["batch_tp.weight"] * tp_size
    tensors = [full_weight] * tp_size
    pars = [TensorParallelism([TP(r, tp_size)]) for r in range(tp_size)]

    store.batch_put_tensor_with_parallelism(
        keys, tensors, parallelisms=pars, tenant=tenant
    )

    targets = [ReadTarget(READ_MODE_SHARD, parallelism=p) for p in pars]
    results = store.batch_get_tensor_with_parallelism(
        keys, targets=targets, tenant=tenant
    )
    assert len(results) == tp_size, f"expected {tp_size} results, got {len(results)}"
    for rank in range(tp_size):
        expected = compute_tp_shard(full_weight, rank, tp_size)
        assert_tensor_equal(results[rank], expected, f"batch tp rank {rank}")


def test_batch_put_get_writer_partition(store, tenant):
    """Batch put via writer_partitions (auto-sliced), read back via batch_get FULL."""
    full_weight = generate_weight([128, 64], seed=51)
    num_writers = 4
    keys = ["batch_wp.weight"] * num_writers
    tensors = [full_weight] * num_writers
    wps = [(r, num_writers, 0) for r in range(num_writers)]

    store.batch_put_tensor_with_parallelism(
        keys, tensors, writer_partitions=wps, tenant=tenant
    )

    result = store.get_tensor_with_parallelism(
        "batch_wp.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "batch wp full roundtrip")


def test_batch_upsert(store, tenant):
    """Batch upsert overwrites previously written data (auto-sliced)."""
    weight_v1 = generate_weight([64, 32], seed=52)
    weight_v2 = generate_weight([64, 32], seed=53)
    tp_size = 2
    keys = ["batch_upsert.weight"] * tp_size
    tensors_v1 = [weight_v1] * tp_size
    tensors_v2 = [weight_v2] * tp_size
    pars = [TensorParallelism([TP(r, tp_size)]) for r in range(tp_size)]

    store.batch_put_tensor_with_parallelism(
        keys, tensors_v1, parallelisms=pars, tenant=tenant
    )
    store.batch_upsert_tensor_with_parallelism(
        keys, tensors_v2, parallelisms=pars, tenant=tenant
    )

    targets = [ReadTarget(READ_MODE_SHARD, parallelism=p) for p in pars]
    results = store.batch_get_tensor_with_parallelism(
        keys, targets=targets, tenant=tenant
    )
    for rank in range(tp_size):
        expected = compute_tp_shard(weight_v2, rank, tp_size)
        assert_tensor_equal(results[rank], expected, f"batch upsert rank {rank}")


def test_batch_mixed_routing_rejected(store, tenant):
    """Passing both parallelisms and writer_partitions in a batch should raise."""
    weight = generate_weight([32, 16], seed=54)
    try:
        store.batch_put_tensor_with_parallelism(
            ["mixed.weight"],
            [weight],
            parallelisms=[TensorParallelism([TP(0, 2)])],
            writer_partitions=[(0, 2, 0)],
            tenant=tenant,
        )
        raise AssertionError("should have raised for mixed parallelisms + writer_partitions")
    except (ValueError, RuntimeError):
        pass


def test_from_put_get_roundtrip(store, tenant):
    """put_tensor_with_parallelism_from: write from raw buffer, read back."""
    weight = generate_weight([64, 32], seed=55)
    buf_ptr, buf_size, buf_ref = build_raw_buffer(weight)

    store.put_tensor_with_parallelism_from(
        "test_from_rt.weight", buf_ptr, buf_size, tenant=tenant
    )

    result = store.get_tensor_with_parallelism(
        "test_from_rt.weight",
        target=ReadTarget(READ_MODE_AS_STORED),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight, "from roundtrip")
    del buf_ref


def test_from_upsert(store, tenant):
    """upsert_tensor_with_parallelism_from: overwrite via raw buffer."""
    weight_v1 = generate_weight([32, 16], seed=56)
    weight_v2 = generate_weight([32, 16], seed=57)

    buf1_ptr, buf1_size, buf1_ref = build_raw_buffer(weight_v1)
    store.put_tensor_with_parallelism_from(
        "test_from_upsert.weight", buf1_ptr, buf1_size, tenant=tenant
    )

    buf2_ptr, buf2_size, buf2_ref = build_raw_buffer(weight_v2)
    store.upsert_tensor_with_parallelism_from(
        "test_from_upsert.weight", buf2_ptr, buf2_size, tenant=tenant
    )

    result = store.get_tensor_with_parallelism(
        "test_from_upsert.weight",
        target=ReadTarget(READ_MODE_AS_STORED),
        tenant=tenant,
    )
    assert_tensor_equal(result, weight_v2, "from upsert should be v2")
    del buf1_ref, buf2_ref


def test_parallelism_manifest_discovery(store, tenant):
    """TP write creates __parallelism_manifest; reader discovers via it."""
    full_weight = generate_weight([128, 64], seed=58)
    tp_size = 4

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_par_manifest.weight", full_weight, parallelism=par, tenant=tenant
        )

    manifest_raw = store.get(
        "test_par_manifest.weight__parallelism_manifest", tenant=tenant
    )
    assert len(manifest_raw) > 0, "parallelism manifest should exist"

    result = store.get_tensor_with_parallelism(
        "test_par_manifest.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "parallelism manifest full reconstruction")


def test_writer_key_naming_format(store, tenant):
    """Writer partition key should use format __writer_{rank}of{size}_sd{split_dim}."""
    weight = generate_weight([64, 32], seed=59)
    store.put_tensor_with_parallelism(
        "test_wk_fmt.weight",
        weight,
        writer_partition=(1, 4, 0),
        tenant=tenant,
    )

    raw = store.get("test_wk_fmt.weight__writer_1of4_sd0", tenant=tenant)
    assert len(raw) > TENSOR_METADATA_WIRE_SIZE, "new writer key format should exist"

    try:
        store.get("test_wk_fmt.weight__writer_1", tenant=tenant)
        raise AssertionError("old writer key format should NOT exist")
    except RuntimeError:
        pass


def test_writer_key_naming_split_dim1(store, tenant):
    """Writer partition key with split_dim=1."""
    weight = generate_weight([32, 64], seed=60)
    store.put_tensor_with_parallelism(
        "test_wk_sd1.weight",
        weight,
        writer_partition=(2, 4, 1),
        tenant=tenant,
    )

    raw = store.get("test_wk_sd1.weight__writer_2of4_sd1", tenant=tenant)
    assert len(raw) > TENSOR_METADATA_WIRE_SIZE, "writer key sd1 format should exist"


def test_batch_get_into_tp(store, allocator, tenant):
    """batch_get_tensor_with_parallelism_into: read TP shards into buffers."""
    full_weight = generate_weight([64, 32], seed=61)
    tp_size = 2

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_batch_into.weight", full_weight, parallelism=par, tenant=tenant
        )

    shard_0 = compute_tp_shard(full_weight, 0, tp_size)
    shard_1 = compute_tp_shard(full_weight, 1, tp_size)
    buf_size = TENSOR_METADATA_WIRE_SIZE + shard_0.nelement() * shard_0.element_size()

    ptr0 = allocator.alloc(buf_size)
    ptr1 = allocator.alloc(buf_size)
    r0 = store.register_buffer(ptr0, buf_size)
    r1 = store.register_buffer(ptr1, buf_size)
    if r0 != 0 or r1 != 0:
        raise SkipTest("register_buffer not supported")

    try:
        par0 = TensorParallelism([TP(0, tp_size)])
        par1 = TensorParallelism([TP(1, tp_size)])
        results = store.batch_get_tensor_with_parallelism_into(
            ["test_batch_into.weight", "test_batch_into.weight"],
            [ptr0, ptr1],
            [buf_size, buf_size],
            targets=[
                ReadTarget(READ_MODE_SHARD, parallelism=par0),
                ReadTarget(READ_MODE_SHARD, parallelism=par1),
            ],
            tenant=tenant,
        )
        assert len(results) == 2, f"expected 2 results, got {len(results)}"

        for idx, (result, expected) in enumerate([(results[0], shard_0), (results[1], shard_1)]):
            assert result.data_bytes == expected.nelement() * expected.element_size(), \
                f"batch_into rank {idx}: data_bytes mismatch"
    finally:
        store.unregister_buffer(ptr0, buf_size)
        store.unregister_buffer(ptr1, buf_size)


def test_batch_get_cross_tp_reconstruction(store, tenant):
    """batch_get with cross-TP: writer TP=2, reader TP=4."""
    full_weight = generate_weight([128, 64], seed=62)
    writer_tp = 2
    reader_tp = 4

    for rank in range(writer_tp):
        par = TensorParallelism([TP(rank, writer_tp)])
        store.put_tensor_with_parallelism(
            "test_batch_cross.weight", full_weight, parallelism=par, tenant=tenant
        )

    keys = ["test_batch_cross.weight"] * reader_tp
    targets = [
        ReadTarget(READ_MODE_SHARD, parallelism=TensorParallelism([TP(r, reader_tp)]))
        for r in range(reader_tp)
    ]
    results = store.batch_get_tensor_with_parallelism(
        keys, targets=targets, tenant=tenant
    )
    for rank in range(reader_tp):
        expected = compute_tp_shard(full_weight, rank, reader_tp)
        assert_tensor_equal(results[rank], expected, f"batch cross tp rank {rank}")


def test_batch_from_put_get(store, tenant):
    """batch_put_tensor_with_parallelism_from: batch raw buffer write + read."""
    weights = [generate_weight([32, 16], seed=63 + i) for i in range(3)]
    buf_refs = []
    ptrs = []
    szs = []
    keys = [f"batch_from.w{i}" for i in range(3)]

    for w in weights:
        p, s, ref = build_raw_buffer(w)
        ptrs.append(p)
        szs.append(s)
        buf_refs.append(ref)

    store.batch_put_tensor_with_parallelism_from(
        keys, ptrs, szs, tenant=tenant
    )

    for i, key in enumerate(keys):
        result = store.get_tensor_with_parallelism(
            key, target=ReadTarget(READ_MODE_AS_STORED), tenant=tenant
        )
        assert_tensor_equal(result, weights[i], f"batch_from key {i}")
    del buf_refs


def test_uniform_shard_validation(store, tenant):
    """Non-uniform shard request should be rejected."""
    weight = generate_weight([30, 16], seed=70)
    par = TensorParallelism([TP(0, 4)])  # 30 not divisible by 4
    try:
        store.put_tensor_with_parallelism(
            "test_uniform_reject.weight", weight, parallelism=par, tenant=tenant
        )
        raise AssertionError("should have raised for non-uniform shard")
    except ValueError:
        pass


def test_writer_partition_shortcut_read(store, tenant):
    """Write via writer_partition, read via TP shard with same size (shortcut path)."""
    full_weight = generate_weight([128, 64], seed=71)
    num_writers = 4
    split_dim = 0

    for rank in range(num_writers):
        store.put_tensor_with_parallelism(
            "test_wp_shortcut.weight",
            full_weight,
            writer_partition=(rank, num_writers, split_dim),
            tenant=tenant,
        )

    for rank in range(num_writers):
        par = TensorParallelism([TP(rank, num_writers)])
        result = store.get_tensor_with_parallelism(
            "test_wp_shortcut.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, num_writers, split_dim=split_dim)
        assert_tensor_equal(result, expected, f"wp shortcut rank {rank}")


def test_from_put_with_parallelism(store, tenant):
    """put_tensor_with_parallelism_from with TP auto-slicing from raw buffer."""
    full_weight = generate_weight([64, 32], seed=72)
    buf_ptr, buf_size, buf_ref = build_raw_buffer(full_weight)
    tp_size = 2

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism_from(
            "test_from_tp.weight", buf_ptr, buf_size,
            parallelism=par, tenant=tenant
        )

    for rank in range(tp_size):
        par = TensorParallelism([TP(rank, tp_size)])
        result = store.get_tensor_with_parallelism(
            "test_from_tp.weight",
            target=ReadTarget(READ_MODE_SHARD, parallelism=par),
            tenant=tenant,
        )
        expected = compute_tp_shard(full_weight, rank, tp_size)
        assert_tensor_equal(result, expected, f"from tp rank {rank}")
    del buf_ref


def test_from_put_with_writer_partition(store, tenant):
    """put_tensor_with_parallelism_from with writer_partition auto-slicing."""
    full_weight = generate_weight([64, 32], seed=73)
    buf_ptr, buf_size, buf_ref = build_raw_buffer(full_weight)
    num_writers = 2

    for rank in range(num_writers):
        store.put_tensor_with_parallelism_from(
            "test_from_wp.weight", buf_ptr, buf_size,
            writer_partition=(rank, num_writers, 0), tenant=tenant
        )

    result = store.get_tensor_with_parallelism(
        "test_from_wp.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "from wp full roundtrip")
    del buf_ref


def main():
    args = parse_args()

    store = MooncakeDistributedStore()
    result = store.setup_dummy(
        64 * 1024 * 1024,
        16 * 1024 * 1024,
        args.daemon_addr,
        keyspace=args.keyspace,
    )
    if result != 0:
        raise RuntimeError(f"setup_dummy failed: result={result}")
    allocator = MooncakeHostMemAllocator()

    tenant = args.tenant

    print("Unified Parallel Tensor IO E2E Tests:")

    print("\n  --- Basic round-trip ---")
    run_test("as_stored roundtrip", test_as_stored_roundtrip, store, tenant)
    run_test("full tensor roundtrip", test_full_tensor_roundtrip, store, tenant)

    print("\n  --- Single TP ---")
    run_test("tp shard exact match", test_tp_shard_exact_match, store, tenant)
    run_test("tp shard legacy compat", test_tp_shard_legacy_compat, store, tenant)

    print("\n  --- Cross-TP reconstruction ---")
    run_test("tp split 4->8", test_tp_split_4to8, store, tenant)
    run_test("tp merge 8->4", test_tp_merge_8to4, store, tenant)
    run_test("full reconstruction from shards", test_full_reconstruction, store, tenant)

    print("\n  --- Multi-axis parallelism ---")
    run_test("dp+tp shard roundtrip", test_dp_tp_shard_roundtrip, store, tenant)
    run_test("dp+tp key format", test_dp_tp_key_format, store, tenant)

    print("\n  --- Upsert ---")
    run_test("upsert overwrites", test_upsert_overwrites, store, tenant)

    print("\n  --- Buffer operations ---")
    run_test("shard read into buffer", test_shard_read_into_buffer, store, allocator, tenant)

    print("\n  --- Error handling ---")
    run_test("invalid axis rejected", test_invalid_axis_rejected, store, tenant)
    run_test("read nonexistent shard", test_read_nonexistent_shard, store, tenant)

    print("\n  --- split_dim & dtype ---")
    run_test("tp split_dim=1", test_tp_split_dim1, store, tenant)
    run_test("dtype bfloat16", test_dtype_bfloat16, store, tenant)
    run_test("dtype float16", test_dtype_float16, store, tenant)

    print("\n  --- Shape edge cases ---")
    run_test("1d tensor tp", test_1d_tensor_tp, store, tenant)
    run_test("tp size=1", test_tp_size_1, store, tenant)

    print("\n  --- Non-TP axes ---")
    run_test("pp only shard", test_pp_only_shard, store, tenant)

    print("\n  --- Read path variants ---")
    run_test("read into preallocated tensor", test_read_into_preallocated_tensor, store, tenant)

    print("\n  --- Large TP fan-out ---")
    run_test("tp split 2->16", test_tp_split_2to16, store, tenant)

    print("\n  --- Upsert variants ---")
    run_test("upsert direct full", test_upsert_direct_full, store, tenant)

    print("\n  --- Cross-TP split_dim=1 ---")
    run_test("split_dim=1 cross 2->4", test_tp_split_dim1_cross_tp, store, tenant)
    run_test("split_dim=1 merge 4->2", test_tp_split_dim1_merge_cross_tp, store, tenant)
    run_test("split_dim=1 full reconstruction", test_tp_split_dim1_full_reconstruction, store, tenant)

    print("\n  --- Writer partition ---")
    run_test("writer partition roundtrip", test_writer_partition_roundtrip, store, tenant)
    run_test("writer partition -> tp read", test_writer_partition_to_tp_read, store, tenant)
    run_test("writer partition split_dim=1", test_writer_partition_split_dim1, store, tenant)
    run_test("writer partition upsert", test_writer_partition_upsert, store, tenant)

    print("\n  --- Mock RL E2E ---")
    run_test("rl weight sync e2e", test_rl_weight_sync_e2e, store, tenant)
    run_test("rl multi-layer sync", test_rl_weight_sync_multi_layer, store, tenant)

    print("\n  --- Batch API ---")
    run_test("batch put+get tp", test_batch_put_get_tp, store, tenant)
    run_test("batch put+get writer partition", test_batch_put_get_writer_partition, store, tenant)
    run_test("batch upsert", test_batch_upsert, store, tenant)
    run_test("batch mixed routing rejected", test_batch_mixed_routing_rejected, store, tenant)
    run_test("batch get cross-tp reconstruction", test_batch_get_cross_tp_reconstruction, store, tenant)

    print("\n  --- _from API ---")
    run_test("from put+get roundtrip", test_from_put_get_roundtrip, store, tenant)
    run_test("from upsert", test_from_upsert, store, tenant)
    run_test("batch from put+get", test_batch_from_put_get, store, tenant)

    print("\n  --- Parallelism manifest ---")
    run_test("parallelism manifest discovery", test_parallelism_manifest_discovery, store, tenant)

    print("\n  --- Writer key naming ---")
    run_test("writer key format", test_writer_key_naming_format, store, tenant)
    run_test("writer key split_dim=1", test_writer_key_naming_split_dim1, store, tenant)

    print("\n  --- Batch into buffer ---")
    run_test("batch get into tp", test_batch_get_into_tp, store, allocator, tenant)

    print("\n  --- Auto-slicing validation ---")
    run_test("uniform shard validation", test_uniform_shard_validation, store, tenant)
    run_test("writer partition shortcut read", test_writer_partition_shortcut_read, store, tenant)

    print("\n  --- _from with parallelism ---")
    run_test("from put with tp", test_from_put_with_parallelism, store, tenant)
    run_test("from put with writer_partition", test_from_put_with_writer_partition, store, tenant)

    store.close()

    print(f"\nResults: {passed} passed, {failed} failed, {skipped} skipped, {passed + failed + skipped} total")
    if failed > 0:
        sys.exit(1)
    print("All tests PASSED.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL: {e}", file=sys.stderr)
        sys.exit(1)
