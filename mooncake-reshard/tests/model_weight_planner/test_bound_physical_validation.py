from __future__ import annotations

from math import prod
from types import SimpleNamespace

import pytest

from mooncake.reshard.weight._planner.bound_contracts import TransferPlan
from mooncake.reshard.weight._planner.bound_validation import (
    _validate_target_physical_ranges,
)
from mooncake.reshard.weight._planner.contracts import TransferRegion
from mooncake.reshard.weight.manifest import ParallelRank

from .helpers import bound_fragment, tp_manifests


class MutableOperation:
    """Deliberately non-canonical operation used to prove the public boundary."""

    def __init__(self, source: object, target: object) -> None:
        self.source = source
        self.target = target

    def iter_segments(self) -> tuple[tuple[int, int, int], ...]:
        return ((0, 0, 2),)

    def validate_bounds(self) -> None:
        return None


def strided_column_region(
    *,
    tensor_id: str,
    fragment_suffix: str,
    target_fragment_id: str,
    target_address: int,
    column: int,
    rows: int = 4,
    columns: int = 2,
    target_instance_id: str = "target-instance",
    target_endpoint: str = "target-worker:12345",
    target_device: str = "cuda:0",
) -> TransferRegion:
    shape = (rows, columns)
    source = bound_fragment(
        fragment_id=f"source-{fragment_suffix}",
        tensor_id=tensor_id,
        global_offset=(0, 0),
        local_shape=shape,
        address=0x30000 + len(fragment_suffix) * 0x100,
        nbytes=prod(shape) * 2,
        worker_id=f"source-{fragment_suffix}",
        endpoint=f"source-{fragment_suffix}:12345",
        device="cuda:0",
        rank=ParallelRank(),
    )
    target = bound_fragment(
        fragment_id=target_fragment_id,
        tensor_id=tensor_id,
        global_offset=(0, 0),
        local_shape=shape,
        address=target_address,
        nbytes=prod(shape) * 2,
        worker_id="target-worker",
        endpoint=target_endpoint,
        device=target_device,
        instance_id=target_instance_id,
        rank=ParallelRank(),
    )
    return TransferRegion(
        tensor_id=tensor_id,
        source=source,
        target=target,
        overlap_offset=(0, column),
        overlap_shape=(rows, 1),
        source_base_offset=column * 2,
        target_base_offset=column * 2,
        inner_bytes=2,
        outer_loop_counts=(rows,),
        source_strides=(columns * 2,),
        target_strides=(columns * 2,),
    )


def test_physical_overlap_is_detected_for_logically_disjoint_regions() -> None:
    left = strided_column_region(
        tensor_id="left",
        fragment_suffix="left",
        target_fragment_id="shared-fragment",
        target_address=0x40000,
        column=0,
    )
    right = strided_column_region(
        tensor_id="right",
        fragment_suffix="right",
        target_fragment_id="shared-fragment",
        target_address=0x40000 - 2,
        column=1,
    )

    with pytest.raises(ValueError, match="conflicting target physical range"):
        _validate_target_physical_ranges((left, right))


def test_public_transfer_plan_rejects_conflicting_target_physical_ranges() -> None:
    """The public executable-plan boundary must not rely on binder-only checks."""

    left = strided_column_region(
        tensor_id="left",
        fragment_suffix="left",
        target_fragment_id="shared-fragment",
        target_address=0x40000,
        column=0,
    )
    right = strided_column_region(
        tensor_id="right",
        fragment_suffix="right",
        target_fragment_id="shared-fragment",
        target_address=0x40000 - 2,
        column=1,
    )

    with pytest.raises(ValueError, match="lacks an attested runtime binding"):
        TransferPlan(
            resource_id="resource",
            revision="revision",
            weight_generation=1,
            target_placement=tp_manifests(
                tp=1,
                pp_rank=0,
                ep_rank=0,
                address_base=0x40000,
                worker_prefix="target-placement",
            ).placement,
            operations=(left, right),
        )


def test_public_transfer_plan_accepts_disjoint_target_physical_ranges() -> None:
    left = strided_column_region(
        tensor_id="left",
        fragment_suffix="left",
        target_fragment_id="target-left",
        target_address=0x40000,
        column=0,
    )
    right = strided_column_region(
        tensor_id="right",
        fragment_suffix="right",
        target_fragment_id="target-right",
        target_address=0x40000,
        column=1,
    )

    with pytest.raises(ValueError, match="lacks an attested runtime binding"):
        TransferPlan(
            resource_id="resource",
            revision="revision",
            weight_generation=1,
            target_placement=tp_manifests(
                tp=1,
                pp_rank=0,
                ep_rank=0,
                address_base=0x40000,
                worker_prefix="target-placement",
            ).placement,
            operations=(left, right),
        )


def test_public_transfer_plan_rejects_mutable_duck_typed_operation() -> None:
    source = bound_fragment(
        fragment_id="source-fake",
        tensor_id="fake",
        global_offset=(0,),
        local_shape=(4,),
        address=0x30000,
        nbytes=8,
        worker_id="source-worker",
        endpoint="source-worker:12345",
        device="cuda:0",
        rank=ParallelRank(),
    )
    target = bound_fragment(
        fragment_id="target-fake",
        tensor_id="fake",
        global_offset=(0,),
        local_shape=(4,),
        address=0x40000,
        nbytes=8,
        worker_id="target-worker",
        endpoint="target-worker:12345",
        device="cuda:0",
        instance_id="target-instance",
        rank=ParallelRank(),
    )

    with pytest.raises(ValueError, match="canonical transfer operation"):
        TransferPlan(
            resource_id="resource",
            revision="revision",
            weight_generation=1,
            target_placement=tp_manifests(
                tp=1,
                pp_rank=0,
                ep_rank=0,
                address_base=0x40000,
                worker_prefix="target-placement",
            ).placement,
            operations=(MutableOperation(source, target),),  # type: ignore[arg-type]
        )


def test_public_transfer_plan_rejects_duck_typed_executor_metadata() -> None:
    region = strided_column_region(
        tensor_id="tensor",
        fragment_suffix="executor",
        target_fragment_id="target-executor",
        target_address=0x40000,
        column=0,
    )
    fake_executor = SimpleNamespace()

    with pytest.raises(ValueError, match="canonical executor metadata"):
        TransferPlan(
            resource_id="resource",
            revision="revision",
            weight_generation=1,
            target_placement=tp_manifests(
                tp=1,
                pp_rank=0,
                ep_rank=0,
                address_base=0x40000,
                worker_prefix="target-placement",
            ).placement,
            operations=(region,),
            source_executors=(fake_executor,),  # type: ignore[arg-type]
        )


def test_interleaved_disjoint_target_segments_are_accepted() -> None:
    left = strided_column_region(
        tensor_id="left",
        fragment_suffix="left",
        target_fragment_id="target-left",
        target_address=0x40000,
        column=0,
    )
    right = strided_column_region(
        tensor_id="right",
        fragment_suffix="right",
        target_fragment_id="target-right",
        target_address=0x40000,
        column=1,
    )

    _validate_target_physical_ranges((left, right))


def test_same_virtual_address_on_different_devices_is_accepted() -> None:
    left = strided_column_region(
        tensor_id="left",
        fragment_suffix="left",
        target_fragment_id="target-left",
        target_address=0x40000,
        column=0,
        target_device="cuda:0",
    )
    right = strided_column_region(
        tensor_id="right",
        fragment_suffix="right",
        target_fragment_id="target-right",
        target_address=0x40000 - 2,
        column=1,
        target_device="cuda:1",
    )

    _validate_target_physical_ranges((left, right))


def test_endpoint_change_does_not_create_a_new_address_space() -> None:
    left = strided_column_region(
        tensor_id="left",
        fragment_suffix="left",
        target_fragment_id="target-left",
        target_address=0x40000,
        column=0,
        target_endpoint="target-route-a:12345",
    )
    right = strided_column_region(
        tensor_id="right",
        fragment_suffix="right",
        target_fragment_id="target-right",
        target_address=0x40000 - 2,
        column=1,
        target_endpoint="target-route-b:12345",
    )

    with pytest.raises(ValueError, match="conflicting target physical range"):
        _validate_target_physical_ranges((left, right))


def test_last_mixed_radix_target_segment_overlap_is_rejected() -> None:
    left = strided_column_region(
        tensor_id="left",
        fragment_suffix="left",
        target_fragment_id="target-left",
        target_address=0x40000,
        column=0,
    )
    right = strided_column_region(
        tensor_id="right",
        fragment_suffix="right",
        target_fragment_id="target-right",
        target_address=0x40000 + 10,
        column=1,
    )

    with pytest.raises(ValueError, match="conflicting target physical range"):
        _validate_target_physical_ranges((left, right))


def test_target_segment_overlap_scan_budget_fails_closed() -> None:
    left = strided_column_region(
        tensor_id="left",
        fragment_suffix="left",
        target_fragment_id="target-left",
        target_address=0x40000,
        column=0,
        rows=8,
    )
    right = strided_column_region(
        tensor_id="right",
        fragment_suffix="right",
        target_fragment_id="target-right",
        target_address=0x40000,
        column=1,
        rows=8,
    )

    with pytest.raises(ValueError, match="segment scan budget"):
        _validate_target_physical_ranges((left, right), max_segment_checks=3)


def test_target_physical_scan_visits_each_emitted_segment_once() -> None:
    rows = 16
    columns = 32
    regions = tuple(
        strided_column_region(
            tensor_id=f"column-{column}",
            fragment_suffix=f"column-{column}",
            target_fragment_id=f"target-column-{column}",
            target_address=0x40000,
            column=column,
            rows=rows,
            columns=columns,
        )
        for column in range(columns)
    )

    _validate_target_physical_ranges(
        regions,
        max_segment_checks=rows * columns,
    )


def test_complete_target_fragment_is_not_rejected_by_global_segment_budget() -> None:
    rows = 500_001
    regions = tuple(
        strided_column_region(
            tensor_id="shared-tensor",
            fragment_suffix=f"column-{column}",
            target_fragment_id="shared-target",
            target_address=0x40000,
            column=column,
            rows=rows,
            columns=2,
        )
        for column in range(2)
    )

    assert sum(region.segment_count for region in regions) > 1_000_000
    _validate_target_physical_ranges(regions)


def test_complete_fragment_and_fallback_region_overlap_is_rejected() -> None:
    complete_regions = tuple(
        strided_column_region(
            tensor_id="complete-tensor",
            fragment_suffix=f"complete-column-{column}",
            target_fragment_id="complete-target",
            target_address=0x40000,
            column=column,
            rows=8,
            columns=2,
        )
        for column in range(2)
    )
    fallback_region = strided_column_region(
        tensor_id="fallback-tensor",
        fragment_suffix="fallback",
        target_fragment_id="fallback-target",
        target_address=0x40000 - 2,
        column=1,
        rows=8,
        columns=2,
    )

    with pytest.raises(ValueError, match="conflicting target physical range"):
        _validate_target_physical_ranges((*complete_regions, fallback_region))
