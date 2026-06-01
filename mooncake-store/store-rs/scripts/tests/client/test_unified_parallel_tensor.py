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
    """Write TP=4 shards, read back with exact same TP=4."""
    full_weight = generate_weight([256, 128], seed=3)
    tp_size = 4

    for rank in range(tp_size):
        shard = compute_tp_shard(full_weight, rank, tp_size)
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_tp_exact.weight", shard, parallelism=par, tenant=tenant
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
        shard = compute_tp_shard(full_weight, rank, trainer_tp)
        par = TensorParallelism([TP(rank, trainer_tp)])
        store.put_tensor_with_parallelism(
            "test_split.weight", shard, parallelism=par, tenant=tenant
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
        shard = compute_tp_shard(full_weight, rank, trainer_tp)
        par = TensorParallelism([TP(rank, trainer_tp)])
        store.put_tensor_with_parallelism(
            "test_merge.weight", shard, parallelism=par, tenant=tenant
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
    """Write TP=4 shards, reconstruct full tensor with ReadTarget(FULL)."""
    full_weight = generate_weight([128, 64], seed=7)
    tp_size = 4

    for rank in range(tp_size):
        shard = compute_tp_shard(full_weight, rank, tp_size)
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_full_recon.weight", shard, parallelism=par, tenant=tenant
        )

    result = store.get_tensor_with_parallelism(
        "test_full_recon.weight",
        target=ReadTarget(READ_MODE_FULL),
        tenant=tenant,
    )
    assert_tensor_equal(result, full_weight, "full reconstruction")


def test_dp_tp_shard_roundtrip(store, tenant):
    """Write DP=2 TP=4 shards, read back with exact match.

    Note: all DP ranks store the same TP shard data here because this test
    validates multi-axis key naming and retrieval, not DP data semantics.
    """
    full_weight = generate_weight([128, 64], seed=8)
    dp_size = 2
    tp_size = 4

    for dp_rank in range(dp_size):
        for tp_rank in range(tp_size):
            shard = compute_tp_shard(full_weight, tp_rank, tp_size)
            par = TensorParallelism([DP(dp_rank, dp_size), TP(tp_rank, tp_size)])
            store.put_tensor_with_parallelism(
                "test_dp_tp.weight", shard, parallelism=par, tenant=tenant
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
    """Upsert should overwrite existing data."""
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
    assert_tensor_equal(result, weight_v2, "upsert should be v2")


def test_shard_read_into_buffer(store, allocator, tenant):
    """Read a shard into a pre-registered buffer."""
    full_weight = generate_weight([64, 32], seed=12)
    tp_size = 2

    for rank in range(tp_size):
        shard = compute_tp_shard(full_weight, rank, tp_size)
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_into_buf.weight", shard, parallelism=par, tenant=tenant
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
        shard = compute_tp_shard(full_weight, tp_rank, trainer_tp)
        par = TensorParallelism([TP(tp_rank, trainer_tp)])
        store.put_tensor_with_parallelism(
            "rl_sync.layers.0.weight", shard, parallelism=par, tenant=tenant
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
            shard = compute_tp_shard(weight, tp_rank, trainer_tp)
            par = TensorParallelism([TP(tp_rank, trainer_tp)])
            store.put_tensor_with_parallelism(
                f"multi_layer.layers.{layer_idx}.weight",
                shard,
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
        shard = compute_tp_shard(full_weight, rank, tp_size, split_dim=1)
        par = TensorParallelism([TP(rank, tp_size, split_dim=1)])
        store.put_tensor_with_parallelism(
            "test_sd1.weight", shard, parallelism=par, tenant=tenant
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
    """bfloat16 tensor round-trip through parallelism API."""
    gen = torch.Generator().manual_seed(21)
    weight = torch.randn([64, 32], dtype=torch.bfloat16, generator=gen)
    par = TensorParallelism([TP(0, 2)])
    shard = compute_tp_shard(weight, 0, 2)
    store.put_tensor_with_parallelism(
        "test_bf16.weight", shard, parallelism=par, tenant=tenant
    )
    result = store.get_tensor_with_parallelism(
        "test_bf16.weight",
        target=ReadTarget(READ_MODE_SHARD, parallelism=par),
        tenant=tenant,
    )
    assert_tensor_equal(result, shard, "bfloat16 roundtrip")


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
    """1D tensor [1024] with TP=4 sharding."""
    full_weight = generate_weight([1024], seed=23)
    tp_size = 4

    for rank in range(tp_size):
        shard = compute_tp_shard(full_weight, rank, tp_size)
        par = TensorParallelism([TP(rank, tp_size)])
        store.put_tensor_with_parallelism(
            "test_1d.weight", shard, parallelism=par, tenant=tenant
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
        shard = compute_tp_shard(full_weight, rank, writer_tp)
        par = TensorParallelism([TP(rank, writer_tp)])
        store.put_tensor_with_parallelism(
            "test_2to16.weight", shard, parallelism=par, tenant=tenant
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

    print("\n  --- Mock RL E2E ---")
    run_test("rl weight sync e2e", test_rl_weight_sync_e2e, store, tenant)
    run_test("rl multi-layer sync", test_rl_weight_sync_multi_layer, store, tenant)

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
