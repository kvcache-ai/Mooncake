from __future__ import annotations

import pytest

from mooncake.reshard.weight._planner.contracts import LogicalTransferPlan
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


def test_public_logical_plan_rejects_mutable_duck_typed_operation() -> None:
    source_placement = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source-placement",
    ).placement
    target_placement = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x20000,
        worker_prefix="target-placement",
    ).placement
    source = bound_fragment(
        fragment_id="source-logical-fake",
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
        fragment_id="target-logical-fake",
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
        LogicalTransferPlan(
            resource_id=source_placement.resource_id,
            revision=source_placement.revision,
            source_placement=source_placement,
            target_placement=target_placement,
            source_tensors=(),
            target_tensors=(),
            operations=(MutableOperation(source, target),),  # type: ignore[arg-type]
        )
