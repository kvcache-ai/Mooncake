from __future__ import annotations

from dataclasses import replace

import pytest

import mooncake.reshard.weight._planner.geometry as geometry_module
from mooncake.reshard.weight._planner.api import plan_placement_transfer_to_local_target
from mooncake.reshard.weight.manifest import (
    ParallelRank,
    PlacementFragment,
    SplitAxis,
)

from .helpers import descriptor, global_placement_from_fragments


def test_expert_box_index_avoids_scanning_every_source_expert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expert_count = 256
    tensor = replace(
        descriptor(),
        tensor_id="layers.2.experts.w1",
        global_shape=(expert_count, 4, 4),
        shard_dims=(0,),
        expert_id=None,
        parallel_axes=(SplitAxis(kind="ep", dim=0),),
    )
    source_fragments = tuple(
        PlacementFragment(
            placement_fragment_id=f"source-e{expert_id}-placement",
            tensor_id=tensor.tensor_id,
            global_offset=(expert_id, 0, 0),
            local_shape=(1, 4, 4),
            nbytes=32,
            rank=ParallelRank(ep=expert_id),
        )
        for expert_id in range(expert_count)
    )
    sources = global_placement_from_fragments(
        resource_id="qwen-moe",
        revision="step-42",
        placement_set_id="source",
        tensors=(tensor,),
        fragments=source_fragments,
    )
    target_expert = 127
    target_fragments = tuple(
        PlacementFragment(
            placement_fragment_id=f"target-e{expert_id}-placement",
            tensor_id=tensor.tensor_id,
            global_offset=(expert_id, 0, 0),
            local_shape=(1, 4, 4),
            nbytes=32,
            rank=ParallelRank(ep=expert_id),
        )
        for expert_id in range(expert_count)
    )
    target = global_placement_from_fragments(
        resource_id="qwen-moe",
        revision="step-42",
        placement_set_id="target",
        tensors=(tensor,),
        fragments=target_fragments,
    )
    overlap_calls = 0
    original_overlap_box = geometry_module._overlap_box

    def counted_overlap_box(source, target):
        nonlocal overlap_calls
        overlap_calls += 1
        return original_overlap_box(source, target)

    monkeypatch.setattr(geometry_module, "_overlap_box", counted_overlap_box)

    plan = plan_placement_transfer_to_local_target(
        sources,
        target,
        f"target-d0-t0-p0-e{target_expert}",
    )

    assert len(plan.operations) == 1
    assert plan.operations[0].source.global_offset == (target_expert, 0, 0)
    assert overlap_calls <= 4
