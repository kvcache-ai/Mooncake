#!/usr/bin/env python3
"""Measure the CPU contract cost of logical planning and runtime binding.

Run from a source checkout:

    PYTHONPATH=mooncake-wheel:python \
      python mooncake-reshard/benchmarks/runtime_binding.py

This is deliberately an opt-in metadata benchmark. It creates no GPU tensors,
does not start Mooncake Store, and does not submit work to Transfer Engine.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import json
from math import prod
import pickle
import platform
import statistics
import time
import tracemalloc
from typing import Callable, TypeVar

from mooncake.reshard.weight import (
    OwnershipAxis,
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    ReplicatedAxis,
    RuntimeBindingFragment,
    SplitAxis,
    TensorDescriptor,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
    bind_logical_transfer_plan,
    plan_placement_transfer,
)
from mooncake.reshard.weight._planner.bound_validation import (
    _validate_target_physical_ranges,
)
from mooncake.reshard.weight._planner.binding import _build_executor_plans


Result = TypeVar("Result")


@dataclass(frozen=True)
class RuntimeInputs:
    placement: WeightPlacementManifest
    bindings: tuple[WeightRuntimeBindingManifest, ...]


def _canonical_strides_bytes(shape: tuple[int, ...], itemsize: int) -> tuple[int, ...]:
    strides: list[int] = []
    running = itemsize
    for extent in reversed(shape):
        strides.append(running)
        running *= extent
    return tuple(reversed(strides))


def _runtime_inputs(
    *,
    tp: int,
    dp: int,
    pp_rank: int,
    ep_rank: int,
    address_base: int,
    label: str,
) -> RuntimeInputs:
    global_shape = (4096, 4096)
    itemsize = 2
    tensor = TensorDescriptor(
        tensor_id="layers.2.experts.3.w1",
        global_shape=global_shape,
        dtype="bfloat16",
        itemsize=itemsize,
        shard_dims=(1,),
        layer_id=2,
        expert_id=3,
        layout_fingerprint="benchmark:qwen3.5:bf16:v1",
        parallel_axes=(
            ReplicatedAxis(kind="dp"),
            OwnershipAxis(kind="pp"),
            OwnershipAxis(kind="ep"),
            SplitAxis(kind="tp", dim=1),
        ),
    )
    shard_width = global_shape[1] // tp
    participants: list[TopologyParticipant] = []
    fragments: list[PlacementFragment] = []
    participant_bindings: list[tuple[str, RuntimeBindingFragment]] = []
    for dp_rank in range(dp):
        for tp_rank in range(tp):
            worker_id = f"{label}-d{dp_rank}-t{tp_rank}"
            rank = ParallelRank(
                dp=dp_rank,
                tp=tp_rank,
                pp=pp_rank,
                ep=ep_rank,
            )
            participant_id = worker_id
            placement_fragment_id = f"{worker_id}-placement"
            local_shape = (global_shape[0], shard_width)
            nbytes = prod(local_shape) * itemsize
            address = address_base + (dp_rank * tp + tp_rank) * (nbytes + 0x1000)
            participants.append(TopologyParticipant(participant_id, rank))
            fragments.append(
                PlacementFragment(
                    placement_fragment_id=placement_fragment_id,
                    tensor_id=tensor.tensor_id,
                    global_offset=(0, tp_rank * shard_width),
                    local_shape=local_shape,
                    nbytes=nbytes,
                    rank=rank,
                )
            )
            participant_bindings.append(
                (
                    participant_id,
                    RuntimeBindingFragment(
                        placement_fragment_id=placement_fragment_id,
                        fragment_id=f"{worker_id}-runtime",
                        address=address,
                        nbytes=nbytes,
                        worker_id=worker_id,
                        endpoint=f"{worker_id}:12345",
                        device="cuda:0",
                        itemsize=itemsize,
                        local_shape=local_shape,
                        strides_bytes=_canonical_strides_bytes(local_shape, itemsize),
                        storage_address=address,
                        storage_nbytes=nbytes,
                        storage_offset_bytes=0,
                    ),
                )
            )

    topology = ParallelTopology(
        tp_size=tp,
        pp_size=pp_rank + 1,
        ep_size=ep_rank + 1,
        dp_size=dp,
        participants=tuple(participants),
    )
    placement = WeightPlacementManifest.from_fragments(
        resource_id="benchmark-qwen3.5-0.8b",
        revision="benchmark-step-42",
        weight_generation=1,
        placement_set_id=label,
        topology=topology,
        tensors=(tensor,),
        fragments=tuple(fragments),
    )
    return RuntimeInputs(
        placement=placement,
        bindings=tuple(
            WeightRuntimeBindingManifest(
                resource_id=placement.resource_id,
                revision=placement.revision,
                placement_id=placement.placement_id,
                placement_digest=placement.digest,
                instance_id=participant_id,
                participant_id=participant_id,
                generation=1,
                lease_id=f"{participant_id}-lease",
                fragments=(binding,),
            )
            for participant_id, binding in participant_bindings
        ),
    )


def _median_ms(
    action: Callable[[], Result], *, warmup: int, repeats: int
) -> tuple[float, Result]:
    for _ in range(warmup):
        action()
    samples: list[float] = []
    result = action()
    for _ in range(repeats):
        started = time.perf_counter_ns()
        result = action()
        samples.append((time.perf_counter_ns() - started) / 1_000_000)
    return statistics.median(samples), result


def _physical_regions(plan, *, rows: int, count: int):
    if rows <= 0 or count <= 0:
        raise ValueError("rows and regions must be positive")
    operation = plan.operations[0]
    if (
        rows > operation.target.local_shape[0]
        or count > operation.target.local_shape[1]
    ):
        raise ValueError("requested physical-region grid exceeds the target fragment")
    itemsize = operation.target.binding.itemsize
    source_row_stride = operation.source.local_shape[1] * itemsize
    target_row_stride = operation.target.local_shape[1] * itemsize
    return tuple(
        replace(
            operation,
            overlap_offset=(0, column),
            overlap_shape=(rows, 1),
            source_base_offset=column * itemsize,
            target_base_offset=column * itemsize,
            inner_bytes=itemsize,
            outer_loop_counts=(rows,),
            source_strides=(source_row_stride,),
            target_strides=(target_row_stride,),
        )
        for column in range(count)
    )


def _measure_peak_bytes(action: Callable[[], object]) -> int:
    tracemalloc.start()
    try:
        action()
        _, peak = tracemalloc.get_traced_memory()
        return peak
    finally:
        tracemalloc.stop()


def _accepts(action: Callable[[], object]) -> bool:
    try:
        action()
    except ValueError:
        return False
    return True


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure Mooncake Reshard logical planning and runtime binding"
    )
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--repeats", type=int, default=31)
    parser.add_argument("--physical-rows", type=int, default=128)
    parser.add_argument("--physical-regions", type=int, default=128)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.warmup < 0 or args.repeats <= 0:
        raise ValueError("warmup must be non-negative and repeats must be positive")
    source = _runtime_inputs(
        tp=4,
        dp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000000,
        label="source",
    )
    target = _runtime_inputs(
        tp=8,
        dp=1,
        pp_rank=1,
        ep_rank=1,
        address_base=0x20000000,
        label="target",
    )
    logical_ms, logical_plan = _median_ms(
        lambda: plan_placement_transfer(source.placement, target.placement),
        warmup=args.warmup,
        repeats=args.repeats,
    )

    def binding_action():
        return bind_logical_transfer_plan(
            logical_plan,
            target.bindings,
            source_bindings=source.bindings,
        )

    binding_ms, transfer_plan = _median_ms(
        binding_action,
        warmup=args.warmup,
        repeats=args.repeats,
    )
    transfer_plan_ms, _ = _median_ms(
        lambda: replace(transfer_plan),
        warmup=args.warmup,
        repeats=args.repeats,
    )
    pickle_dump_ms, encoded = _median_ms(
        lambda: pickle.dumps(transfer_plan, protocol=pickle.HIGHEST_PROTOCOL),
        warmup=args.warmup,
        repeats=args.repeats,
    )
    pickle_restore_ms, _ = _median_ms(
        lambda: pickle.loads(encoded),
        warmup=args.warmup,
        repeats=args.repeats,
    )
    physical_regions = _physical_regions(
        transfer_plan,
        rows=args.physical_rows,
        count=args.physical_regions,
    )
    physical_ms, _ = _median_ms(
        lambda: _validate_target_physical_ranges(
            physical_regions,
            max_segment_checks=sum(region.segment_count for region in physical_regions),
        ),
        warmup=args.warmup,
        repeats=args.repeats,
    )
    selected_source_fragment_ids = frozenset(
        operation.source.fragment_id for operation in transfer_plan.operations
    )
    selected_projection_ms, selected_source_projection = _median_ms(
        lambda: _build_executor_plans(
            (source.placement,),
            source.bindings,
            "source",
            selected_fragment_ids=selected_source_fragment_ids,
        ),
        warmup=args.warmup,
        repeats=args.repeats,
    )
    complete_projection_ms, complete_source_projection = _median_ms(
        lambda: _build_executor_plans(
            (source.placement,),
            source.bindings,
            "source",
        ),
        warmup=args.warmup,
        repeats=args.repeats,
    )
    logical_segment_budget = sum(
        operation.segment_count for operation in logical_plan.operations
    )
    accepted_limits = replace(
        logical_plan.planning_limits,
        max_total_lowered_segments=logical_segment_budget,
    )
    rejected_limits = replace(
        logical_plan.planning_limits,
        max_total_lowered_segments=logical_segment_budget - 1,
    )
    physical_segment_budget = sum(region.segment_count for region in physical_regions)

    result = {
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "processor": platform.processor() or "unknown",
        },
        "topology": {
            "source": {"tp": 4, "pp": 1, "ep": 1, "dp": 2},
            "target": {"tp": 8, "pp": 2, "ep": 2, "dp": 1},
        },
        "workload": {
            "source_fragments": len(source.placement.fragments),
            "target_fragments": len(target.placement.fragments),
            "operations": len(transfer_plan.operations),
            "selected_source_executors": len(transfer_plan.source_executors),
            "selected_target_executors": len(transfer_plan.target_executors),
            "complete_source_executors": len(complete_source_projection),
            "physical_regions": len(physical_regions),
            "physical_segments": physical_segment_budget,
            "warmup": args.warmup,
            "repeats": args.repeats,
        },
        "median_ms": {
            "logical_planning": logical_ms,
            "runtime_binding": binding_ms,
            "transfer_plan_revalidation": transfer_plan_ms,
            "pickle_dump": pickle_dump_ms,
            "pickle_restore": pickle_restore_ms,
            "physical_validation": physical_ms,
            "selected_source_projection": selected_projection_ms,
            "complete_source_projection": complete_projection_ms,
        },
        "peak_bytes": {"runtime_binding": _measure_peak_bytes(binding_action)},
        "projection": {
            "selected_source_executors": len(selected_source_projection),
            "complete_source_executors": len(complete_source_projection),
            "unselected_source_executors": len(complete_source_projection)
            - len(selected_source_projection),
        },
        "planning_limits": {
            "logical_required_segments": logical_segment_budget,
            "accepted_budget": accepted_limits.max_total_lowered_segments,
            "accepted": _accepts(
                lambda: bind_logical_transfer_plan(
                    replace(logical_plan, planning_limits=accepted_limits),
                    target.bindings,
                    source_bindings=source.bindings,
                )
            ),
            "rejected_budget": rejected_limits.max_total_lowered_segments,
            "rejected": not _accepts(
                lambda: bind_logical_transfer_plan(
                    replace(logical_plan, planning_limits=rejected_limits),
                    target.bindings,
                    source_bindings=source.bindings,
                )
            ),
            "physical_required_segments": physical_segment_budget,
            "physical_accepted": _accepts(
                lambda: _validate_target_physical_ranges(
                    physical_regions,
                    max_segment_checks=physical_segment_budget,
                )
            ),
            "physical_rejected": not _accepts(
                lambda: _validate_target_physical_ranges(
                    physical_regions,
                    max_segment_checks=physical_segment_budget - 1,
                )
            ),
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
