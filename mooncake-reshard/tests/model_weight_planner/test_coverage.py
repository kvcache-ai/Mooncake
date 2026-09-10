from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight import plan_placement_transfer

from .helpers import (
    combine_runtime_inputs,
    descriptor,
    rebuild_placement,
    tp_manifests,
)


def test_logical_plan_rejects_target_omitting_a_source_tensor() -> None:
    primary = descriptor()
    secondary = replace(
        primary,
        tensor_id="layers.3.experts.4.w1",
        layer_id=3,
        expert_id=4,
    )
    source = combine_runtime_inputs(
        tp_manifests(
            tp=1,
            pp_rank=0,
            ep_rank=0,
            address_base=0x10000,
            worker_prefix="source-primary",
            tensor=primary,
        ),
        tp_manifests(
            tp=1,
            pp_rank=0,
            ep_rank=0,
            address_base=0x20000,
            worker_prefix="source-secondary",
            tensor=secondary,
        ),
    )
    target = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x30000,
        worker_prefix="target",
        tensor=primary,
    )

    with pytest.raises(ValueError, match="target manifests are missing tensors"):
        plan_placement_transfer(source.placement, target.placement)


def test_global_manifest_barrier_rejects_tensor_missing_from_one_dp_replica() -> None:
    primary = descriptor()
    secondary = replace(
        primary,
        tensor_id="layers.3.experts.4.w1",
        layer_id=3,
        expert_id=4,
    )
    complete = combine_runtime_inputs(
        tp_manifests(
            tp=1,
            dp=2,
            pp_rank=0,
            ep_rank=0,
            address_base=0x10000,
            worker_prefix="target-primary",
            tensor=primary,
        ),
        tp_manifests(
            tp=1,
            dp=2,
            pp_rank=0,
            ep_rank=0,
            address_base=0x50000,
            worker_prefix="target-secondary",
            tensor=secondary,
        ),
    )
    incomplete_fragments = tuple(
        fragment
        for fragment in complete.fragments
        if not (fragment.tensor_id == secondary.tensor_id and fragment.rank.dp == 1)
    )

    with pytest.raises(
        ValueError,
        match=("global placement tensor is not fully covered:.*layers.3.experts.4.w1"),
    ):
        rebuild_placement(complete.placement, fragments=incomplete_fragments)


def test_global_manifest_barrier_rejects_target_tp_gap() -> None:
    target = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x30000,
        worker_prefix="target",
    )

    with pytest.raises(
        ValueError, match="global placement tensor is not fully covered"
    ):
        rebuild_placement(target.placement, fragments=target.fragments[:1])


def test_global_manifest_barrier_rejects_missing_source_coverage() -> None:
    source = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )

    with pytest.raises(
        ValueError, match="global placement tensor is not fully covered"
    ):
        rebuild_placement(source.placement, fragments=source.fragments[:1])
