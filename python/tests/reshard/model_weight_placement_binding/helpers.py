from __future__ import annotations

from mooncake.reshard.weight import (
    OwnershipAxis,
    ParallelRank,
    PlacementFragment,
    ReplicatedAxis,
    SplitAxis,
    RuntimeBindingFragment,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
    TensorDescriptor,
)
from model_weight_planner.helpers import global_placement_from_fragments


MODEL_ID = "model"
REVISION = "revision"
TARGET_PARTICIPANT_ID = "target-tp1"


def descriptor(
    *,
    tensor_id: str = "layers.0.weight",
    global_shape: tuple[int, ...] = (8,),
    shard_dims: tuple[int, ...] = (0,),
    layer_id: int | None = 0,
    expert_id: int | None = None,
) -> TensorDescriptor:
    parallel_axes = tuple(
        [ReplicatedAxis(kind="dp"), OwnershipAxis(kind="pp")]
        + ([OwnershipAxis(kind="ep")] if expert_id is not None else [])
        + [SplitAxis(kind="tp", dim=dim) for dim in shard_dims]
    )
    return TensorDescriptor(
        tensor_id=tensor_id,
        global_shape=global_shape,
        dtype="uint8",
        itemsize=1,
        shard_dims=shard_dims,
        layer_id=layer_id,
        expert_id=expert_id,
        layout_fingerprint="test:logical-box:v2",
        parallel_axes=parallel_axes,
    )


def source_placement(
    *,
    fragment_id: str = "source-placement-fragment",
) -> WeightPlacementManifest:
    tensor = descriptor()
    fragment = PlacementFragment(
        placement_fragment_id=fragment_id,
        tensor_id=tensor.tensor_id,
        global_offset=(0,),
        local_shape=(8,),
        nbytes=8,
        rank=ParallelRank(),
    )
    return global_placement_from_fragments(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_set_id="source",
        tensors=(tensor,),
        fragments=(fragment,),
        participant_ids={fragment.rank: "source-dp0"},
    )


def replicated_source_placement() -> WeightPlacementManifest:
    tensor = descriptor()
    fragments = tuple(
        PlacementFragment(
            placement_fragment_id=f"source-dp{dp}-fragment",
            tensor_id=tensor.tensor_id,
            global_offset=(0,),
            local_shape=(8,),
            nbytes=8,
            rank=ParallelRank(dp=dp),
        )
        for dp in range(2)
    )
    return global_placement_from_fragments(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_set_id="source-replicated",
        tensors=(tensor,),
        fragments=fragments,
        participant_ids={
            fragment.rank: f"source-dp{fragment.rank.dp}" for fragment in fragments
        },
    )


def source_binding(
    *,
    placement: WeightPlacementManifest | None = None,
    placement_id: str | None = None,
    placement_fragment_id: str = "source-placement-fragment",
    instance_id: str = "source-instance",
    generation: int = 3,
    lease_id: str = "source-lease",
    address: int = 0x1000,
    nbytes: int = 8,
    worker_id: str = "source-worker",
    endpoint: str = "source-endpoint",
) -> WeightRuntimeBindingManifest:
    placement = placement or source_placement(
        fragment_id=placement_fragment_id,
    )
    participant_id = next(
        part.participant_id
        for part in placement.parts
        if any(
            fragment.placement_fragment_id == placement_fragment_id
            for fragment in part.fragments
        )
    )
    return WeightRuntimeBindingManifest(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_id=placement_id or placement.placement_id,
        placement_digest=placement.digest,
        instance_id=instance_id,
        participant_id=participant_id,
        generation=generation,
        lease_id=lease_id,
        fragments=(
            RuntimeBindingFragment(
                placement_fragment_id=placement_fragment_id,
                fragment_id=f"{instance_id}-fragment",
                address=address,
                nbytes=nbytes,
                worker_id=worker_id,
                endpoint=endpoint,
                device="cuda:0",
                itemsize=1,
                local_shape=next(
                    fragment.local_shape
                    for fragment in placement.fragments
                    if fragment.placement_fragment_id == placement_fragment_id
                ),
                strides_bytes=(1,),
                storage_address=address,
                storage_nbytes=nbytes,
                storage_offset_bytes=0,
            ),
        ),
    )


def target_placement(
    *,
    fragment_id: str = "placement-fragment",
) -> WeightPlacementManifest:
    tensor = descriptor()
    fragments = (
        PlacementFragment(
            placement_fragment_id="target-complement-fragment",
            tensor_id=tensor.tensor_id,
            global_offset=(0,),
            local_shape=(4,),
            nbytes=4,
            rank=ParallelRank(tp=0),
        ),
        PlacementFragment(
            placement_fragment_id=fragment_id,
            tensor_id=tensor.tensor_id,
            global_offset=(4,),
            local_shape=(4,),
            nbytes=4,
            rank=ParallelRank(tp=1),
        ),
    )
    return global_placement_from_fragments(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_set_id="target",
        tensors=(tensor,),
        fragments=fragments,
        participant_ids={
            ParallelRank(tp=0): "target-tp0",
            ParallelRank(tp=1): TARGET_PARTICIPANT_ID,
        },
    )


def target_binding(
    *,
    placement: WeightPlacementManifest | None = None,
    placement_id: str | None = None,
    placement_fragment_id: str = "placement-fragment",
    nbytes: int = 4,
) -> WeightRuntimeBindingManifest:
    placement = placement or target_placement(fragment_id=placement_fragment_id)
    return WeightRuntimeBindingManifest(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_id=placement_id or placement.placement_id,
        placement_digest=placement.digest,
        instance_id="target-instance",
        participant_id=TARGET_PARTICIPANT_ID,
        generation=7,
        lease_id="target-lease",
        fragments=(
            RuntimeBindingFragment(
                placement_fragment_id=placement_fragment_id,
                fragment_id="target-runtime-fragment",
                address=0x9000,
                nbytes=nbytes,
                worker_id="target-worker",
                endpoint="target-endpoint",
                device="cuda:0",
                itemsize=1,
                local_shape=(4,),
                strides_bytes=(1,),
                storage_address=0x9000,
                storage_nbytes=nbytes,
                storage_offset_bytes=0,
            ),
        ),
    )


def split_target_placement() -> WeightPlacementManifest:
    tensor = descriptor()
    fragments = (
        PlacementFragment(
            placement_fragment_id="placement-left",
            tensor_id=tensor.tensor_id,
            global_offset=(0,),
            local_shape=(4,),
            nbytes=4,
            rank=ParallelRank(),
        ),
        PlacementFragment(
            placement_fragment_id="placement-right",
            tensor_id=tensor.tensor_id,
            global_offset=(4,),
            local_shape=(4,),
            nbytes=4,
            rank=ParallelRank(),
        ),
    )
    return global_placement_from_fragments(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_set_id="target-split",
        tensors=(tensor,),
        fragments=fragments,
        participant_ids={ParallelRank(): "target-split"},
    )


def split_target_binding(
    *,
    right_address: int,
    right_worker_id: str = "target-worker",
    right_endpoint: str = "target-endpoint",
) -> WeightRuntimeBindingManifest:
    placement = split_target_placement()
    return WeightRuntimeBindingManifest(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_id=placement.placement_id,
        placement_digest=placement.digest,
        instance_id="target-instance",
        participant_id="target-split",
        generation=7,
        lease_id="target-lease",
        fragments=(
            RuntimeBindingFragment(
                placement_fragment_id="placement-left",
                fragment_id="runtime-left",
                address=0x9000,
                nbytes=4,
                worker_id="target-worker",
                endpoint="target-endpoint",
                device="cuda:0",
                itemsize=1,
                local_shape=(4,),
                strides_bytes=(1,),
                storage_address=0x9000,
                storage_nbytes=4,
                storage_offset_bytes=0,
            ),
            RuntimeBindingFragment(
                placement_fragment_id="placement-right",
                fragment_id="runtime-right",
                address=right_address,
                nbytes=4,
                worker_id=right_worker_id,
                endpoint=right_endpoint,
                device="cuda:0",
                itemsize=1,
                local_shape=(4,),
                strides_bytes=(1,),
                storage_address=right_address,
                storage_nbytes=4,
                storage_offset_bytes=0,
            ),
        ),
    )
