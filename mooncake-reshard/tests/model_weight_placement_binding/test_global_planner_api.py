from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    TensorDescriptor,
    SplitAxis,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightPlacementPart,
    plan_placement_transfer,
    plan_stored_transfer_to_target_placement,
)
from mooncake.reshard.weight.storage_manifest import (
    StoredFragmentSnapshot,
    StoredWeightManifest,
)


def _placement(
    tp_size: int,
    placement_set_id: str,
    *,
    canonical_fragment_ids: bool = False,
) -> WeightPlacementManifest:
    participants = tuple(
        TopologyParticipant(f"{placement_set_id}-worker-{rank}", ParallelRank(tp=rank))
        for rank in range(tp_size)
    )
    topology = ParallelTopology(
        tp_size=tp_size,
        pp_size=1,
        ep_size=1,
        dp_size=1,
        participants=participants,
    )
    tensor = TensorDescriptor(
        tensor_id="layers.0.weight",
        global_shape=(8,),
        dtype="uint8",
        itemsize=1,
        shard_dims=(0,),
        layout_fingerprint="global-planner:uint8:v1",
        parallel_axes=(SplitAxis(kind="tp", dim=0),),
    )
    extent = 8 // tp_size
    parts = tuple(
        WeightPlacementPart(
            resource_id="model",
            revision="revision",
            weight_generation=9,
            placement_set_id=placement_set_id,
            topology_id=topology.topology_id,
            participant_id=participant.participant_id,
            rank=participant.rank,
            tensors=(tensor,),
            fragments=(
                PlacementFragment(
                    placement_fragment_id=(
                        None
                        if canonical_fragment_ids
                        else f"{placement_set_id}-fragment-{participant.rank.tp}"
                    ),
                    tensor_id=tensor.tensor_id,
                    global_offset=(participant.rank.tp * extent,),
                    local_shape=(extent,),
                    nbytes=extent,
                    rank=participant.rank,
                ),
            ),
        )
        for participant in participants
    )
    return WeightPlacementManifest(
        resource_id="model",
        revision="revision",
        weight_generation=9,
        placement_set_id=placement_set_id,
        topology=topology,
        parts=parts,
    )


def test_planner_accepts_one_complete_source_and_target_manifest() -> None:
    source = _placement(4, "source")
    target = _placement(8, "target")

    plan = plan_placement_transfer(source, target)

    assert len(plan.operations) == 8
    assert {item.placement_id for item in plan.source_executors} == {
        source.placement_id
    }
    assert {item.placement_id for item in plan.target_executors} == {
        target.placement_id
    }


def test_planner_accepts_canonical_fragment_ids_generated_by_manifest() -> None:
    source = _placement(4, "source", canonical_fragment_ids=True)
    target = _placement(8, "target", canonical_fragment_ids=True)

    plan = plan_placement_transfer(source, target)

    assert len(plan.operations) == 8
    assert all(
        fragment.placement_fragment_id.startswith("sha256:")
        for fragment in source.fragments + target.fragments
    )


def test_logical_plan_rejects_forged_placement_source_provenance() -> None:
    source = _placement(2, "source")
    target = _placement(1, "target")
    plan = plan_placement_transfer(source, target)

    forged_source = replace(plan.operations[0].source, rank=ParallelRank(pp=99))
    forged_operation = replace(plan.operations[0], source=forged_source)

    with pytest.raises(
        ValueError,
        match="source placement fragment snapshots differ",
    ):
        replace(plan, operations=(forged_operation, *plan.operations[1:]))

    with pytest.raises(ValueError, match="no source executor metadata"):
        replace(plan, source_executors=())

    with pytest.raises(ValueError, match="source tensor catalog differs"):
        replace(plan, source_tensors=())

    with pytest.raises(ValueError, match="target tensor catalog differs"):
        replace(plan, target_tensors=())


def _stored_source(tensor: TensorDescriptor) -> StoredWeightManifest:
    group_id = "weights/default/model/revision/9"
    return StoredWeightManifest(
        namespace="default",
        resource_id="model",
        revision="revision",
        weight_generation=9,
        group_id=group_id,
        manifest_key=f"{group_id}/manifest",
        created_at="2026-08-19T00:00:00Z",
        tensors=(tensor,),
        fragments=(
            StoredFragmentSnapshot(
                fragment_id="store-source-full",
                tensor_id=tensor.tensor_id,
                global_offset=(0,),
                local_shape=tensor.global_shape,
                object_key=f"{group_id}/payload/0",
                object_offset=0,
                nbytes=8,
            ),
        ),
    )


def _two_tensor_placement(placement_set_id: str) -> WeightPlacementManifest:
    participant = TopologyParticipant(f"{placement_set_id}-worker-0", ParallelRank())
    topology = ParallelTopology(
        tp_size=1,
        pp_size=1,
        ep_size=1,
        dp_size=1,
        participants=(participant,),
    )
    tensors = tuple(
        TensorDescriptor(
            tensor_id=tensor_id,
            global_shape=(8,),
            dtype="uint8",
            itemsize=1,
            shard_dims=(0,),
            layout_fingerprint="global-planner:uint8:v1",
            parallel_axes=(SplitAxis(kind="tp", dim=0),),
        )
        for tensor_id in ("layers.a.weight", "layers.b.weight")
    )
    return WeightPlacementManifest(
        resource_id="model",
        revision="revision",
        weight_generation=9,
        placement_set_id=placement_set_id,
        topology=topology,
        parts=(
            WeightPlacementPart(
                resource_id="model",
                revision="revision",
                weight_generation=9,
                placement_set_id=placement_set_id,
                topology_id=topology.topology_id,
                participant_id=participant.participant_id,
                rank=participant.rank,
                tensors=tensors,
                fragments=tuple(
                    PlacementFragment(
                        placement_fragment_id=f"{placement_set_id}-{tensor.tensor_id}",
                        tensor_id=tensor.tensor_id,
                        global_offset=(0,),
                        local_shape=tensor.global_shape,
                        nbytes=8,
                        rank=participant.rank,
                    )
                    for tensor in tensors
                ),
            ),
        ),
    )


def test_store_backed_planner_attests_selected_source_fragments() -> None:
    target = _placement(8, "target")
    source = _stored_source(target.tensors[0])

    plan = plan_stored_transfer_to_target_placement(source, target)

    assert len(plan.operations) == 8
    assert plan.source_manifest == source
    assert plan.source_manifest_identity == source.manifest_identity
    assert {operation.source for operation in plan.operations} == {source.fragments[0]}

    forged_fragment = replace(
        source.fragments[0],
        object_key="another-tenant/private-object",
    )
    forged_operation = replace(plan.operations[0], source=forged_fragment)
    with pytest.raises(
        ValueError,
        match="source manifest fragment snapshots differ",
    ):
        replace(plan, operations=(forged_operation, *plan.operations[1:]))

    with pytest.raises(ValueError, match="weight_generation differ"):
        plan_stored_transfer_to_target_placement(
            replace(source, weight_generation=10),
            target,
        )


def test_store_backed_planner_accepts_reordered_tensor_catalog() -> None:
    target = _two_tensor_placement("target")
    tensors = target.tensors
    group_id = "weights/default/model/revision/9"
    source = StoredWeightManifest(
        namespace="default",
        resource_id="model",
        revision="revision",
        weight_generation=9,
        group_id=group_id,
        manifest_key=f"{group_id}/manifest",
        created_at="2026-08-20T00:00:00Z",
        tensors=(tensors[1], tensors[0]),
        fragments=tuple(
            StoredFragmentSnapshot(
                fragment_id=f"store-{tensor.tensor_id}",
                tensor_id=tensor.tensor_id,
                global_offset=(0,),
                local_shape=tensor.global_shape,
                object_key=f"{group_id}/payload/{index}",
                object_offset=0,
                nbytes=8,
            )
            for index, tensor in enumerate((tensors[1], tensors[0]))
        ),
    )

    plan = plan_stored_transfer_to_target_placement(source, target)

    assert plan.source_manifest == source
    assert plan.source_tensors == target.tensors
    assert len(plan.operations) == 2
