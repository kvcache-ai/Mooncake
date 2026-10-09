from __future__ import annotations

import pytest

from .helpers import descriptor, operation_for_target, plan_transfer, tp_manifests


def test_tp2_to_tp4_splits_source_fragments_without_full_tensor() -> None:
    source = tp_manifests(
        tp=2,
        pp_rank=1,
        ep_rank=1,
        address_base=0x10000,
        worker_prefix="source",
    )
    target = tp_manifests(
        tp=4,
        pp_rank=2,
        ep_rank=3,
        address_base=0x20000,
        worker_prefix="target",
    )

    plan = plan_transfer(source, target)

    assert len(plan.operations) == 4
    assert [
        (op.source_offset, op.target_offset, op.nbytes)
        for op in operation_for_target(plan, 0)
    ] == [(0, 0, 16)]
    assert [
        (op.source_offset, op.target_offset, op.nbytes)
        for op in operation_for_target(plan, 1)
    ] == [(16, 0, 16)]
    assert operation_for_target(plan, 0)[0].source.rank.pp == 1
    assert operation_for_target(plan, 0)[0].target.rank.pp == 2
    assert operation_for_target(plan, 0)[0].source.rank.ep == 1
    assert operation_for_target(plan, 0)[0].target.rank.ep == 3


def test_tp4_to_tp2_merges_into_non_overlapping_target_offsets() -> None:
    source = tp_manifests(
        tp=4,
        pp_rank=2,
        ep_rank=3,
        address_base=0x10000,
        worker_prefix="source",
    )
    target = tp_manifests(
        tp=2,
        pp_rank=1,
        ep_rank=1,
        address_base=0x20000,
        worker_prefix="target",
    )

    plan = plan_transfer(source, target)

    assert [
        (op.source_offset, op.target_offset, op.nbytes)
        for op in operation_for_target(plan, 0)
    ] == [
        (0, 0, 16),
        (0, 16, 16),
    ]


@pytest.mark.parametrize(("source_tp", "target_tp"), [(4, 8), (8, 4)])
def test_single_axis_manifest_tp4_tp8_same_dim_reshard(
    source_tp: int, target_tp: int
) -> None:
    sources = tp_manifests(
        tp=source_tp,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    targets = tp_manifests(
        tp=target_tp,
        pp_rank=0,
        ep_rank=0,
        address_base=0x20000,
        worker_prefix="target",
    )

    plan = plan_transfer(sources, targets)

    assert len(plan.operations) == 8
    assert all(operation.segment_count == 1 for operation in plan.operations)
    for target in targets:
        fragment = target.fragments[0]
        operations = sorted(
            operation_for_target(plan, fragment.rank.tp),
            key=lambda operation: operation.target_offset,
        )
        cursor = 0
        for operation in operations:
            assert operation.target_offset == cursor
            cursor += operation.nbytes
        assert cursor == fragment.nbytes


def test_shard_dim_one_generates_row_ranges() -> None:
    tensor = descriptor(global_shape=(2, 8), shard_dim=1)
    source = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
        tensor=tensor,
    )
    target = tp_manifests(
        tp=4,
        pp_rank=0,
        ep_rank=0,
        address_base=0x20000,
        worker_prefix="target",
        tensor=tensor,
    )

    plan = plan_transfer(source, target)

    operations = operation_for_target(plan, 0)
    assert len(operations) == 1
    assert operations[0].repeat == 2
    assert operations[0].source_stride == 8
    assert operations[0].target_stride == 4
    assert list(
        operations[0].iter_segments(max_segments=operations[0].segment_count)
    ) == [
        (0, 0, 4),
        (8, 4, 4),
    ]


def test_large_inner_axis_partition_uses_compact_strided_ranges() -> None:
    tensor = descriptor(global_shape=(8192, 8192), shard_dim=1)
    source = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
        tensor=tensor,
    )
    target = tp_manifests(
        tp=4,
        pp_rank=0,
        ep_rank=0,
        address_base=0x20000,
        worker_prefix="target",
        tensor=tensor,
    )

    plan = plan_transfer(source, target)

    assert len(plan.operations) == 4
    assert {operation.repeat for operation in plan.operations} == {8192}
    assert plan.total_bytes == 8192 * 8192 * 2
