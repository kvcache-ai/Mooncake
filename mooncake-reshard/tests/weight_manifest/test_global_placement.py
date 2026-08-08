from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorParallelAxis,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightPlacementPart,
    WeightRuntimeBindingManifest,
    validate_runtime_binding,
    validate_runtime_bindings,
)

from .helpers import (
    MODEL_ID,
    PLACEMENT_SET_ID,
    REVISION,
    WEIGHT_GENERATION,
    _contiguous_strides_bytes,
    descriptor,
    parallel_topology,
    placement_manifest,
    placement_part,
)


def _topology() -> ParallelTopology:
    return parallel_topology(
        participants=(
            TopologyParticipant("worker-0", ParallelRank(tp=0)),
            TopologyParticipant("worker-1", ParallelRank(tp=1)),
        ),
    )


def _part(
    participant_id: str,
    rank: ParallelRank,
    offset: int,
    *,
    placement_set_id: str = "placement-set-7",
    weight_generation: int = 7,
    topology_id: str | None = None,
) -> WeightPlacementPart:
    topology = _topology()
    tensor = descriptor(global_shape=(8, 4))
    return placement_part(
        topology=topology,
        resource_id=MODEL_ID,
        revision=REVISION,
        weight_generation=weight_generation,
        placement_set_id=placement_set_id,
        topology_id=topology_id or topology.topology_id,
        participant_id=participant_id,
        rank=rank,
        tensors=(tensor,),
        fragments=(
            PlacementFragment(
                placement_fragment_id=f"fragment-{participant_id}",
                tensor_id=tensor.tensor_id,
                global_offset=(offset, 0),
                local_shape=(4, 4),
                nbytes=32,
                rank=rank,
            ),
        ),
    )


def _manifest(
    *,
    topology: ParallelTopology | None = None,
    parts: tuple[WeightPlacementPart, ...] | None = None,
) -> WeightPlacementManifest:
    topology = topology or _topology()
    selected_parts = (
        parts
        if parts is not None
        else (
            _part("worker-0", ParallelRank(tp=0), 0),
            _part("worker-1", ParallelRank(tp=1), 4),
        )
    )
    return placement_manifest(
        resource_id=MODEL_ID,
        revision=REVISION,
        weight_generation=WEIGHT_GENERATION,
        placement_set_id=PLACEMENT_SET_ID,
        topology=topology,
        parts=selected_parts,
    )


def test_weight_placement_manifest_is_one_complete_global_placement() -> None:
    placement = _manifest()

    assert placement.topology.world_size == 2
    assert len(placement.parts) == 2
    assert len(placement.tensors) == 1
    assert len(placement.fragments) == 2
    assert WeightPlacementManifest.from_json(placement.to_json()) == placement


def test_placement_part_rejects_unreferenced_tensor_descriptors() -> None:
    with pytest.raises(ValueError, match="unreferenced tensor"):
        placement_part(tensors=(descriptor(),), fragments=())


def test_global_placement_caches_digest_for_all_participant_bindings(
    monkeypatch,
) -> None:
    placement = _manifest()
    original = WeightPlacementManifest.to_json
    calls = 0

    def counted_to_json(self):
        nonlocal calls
        calls += 1
        return original(self)

    monkeypatch.setattr(WeightPlacementManifest, "to_json", counted_to_json)

    assert placement.digest == placement.digest
    assert calls == 1


def test_topology_uses_explicit_participants_instead_of_axis_product() -> None:
    topology = ParallelTopology(
        tp_size=2,
        pp_size=1,
        ep_size=2,
        dp_size=1,
        participants=(
            TopologyParticipant("worker-0", ParallelRank(tp=0, ep=0)),
            TopologyParticipant("worker-1", ParallelRank(tp=1, ep=1)),
        ),
    )

    assert topology.world_size == 2
    assert topology.world_size != topology.tp_size * topology.ep_size


def test_global_placement_may_select_one_complete_dp_replica() -> None:
    topology = ParallelTopology(
        tp_size=2,
        pp_size=4,
        ep_size=1,
        dp_size=2,
        participants=(
            TopologyParticipant("worker-0", ParallelRank(dp=0, tp=0, pp=1)),
            TopologyParticipant("worker-1", ParallelRank(dp=0, tp=1, pp=1)),
        ),
    )
    placement = _manifest(
        topology=topology,
        parts=(
            _part(
                "worker-0",
                ParallelRank(dp=0, tp=0, pp=1),
                0,
                topology_id=topology.topology_id,
            ),
            _part(
                "worker-1",
                ParallelRank(dp=0, tp=1, pp=1),
                4,
                topology_id=topology.topology_id,
            ),
        ),
    )

    assert placement.topology.dp_size == 2
    assert {part.rank.dp for part in placement.parts} == {0}


def test_global_placement_rejects_an_empty_selected_dp_replica() -> None:
    topology = ParallelTopology(
        tp_size=2,
        pp_size=1,
        ep_size=1,
        dp_size=2,
        participants=(
            TopologyParticipant("dp0-tp0", ParallelRank(dp=0, tp=0)),
            TopologyParticipant("dp0-tp1", ParallelRank(dp=0, tp=1)),
            TopologyParticipant("dp1-tp0", ParallelRank(dp=1, tp=0)),
            TopologyParticipant("dp1-tp1", ParallelRank(dp=1, tp=1)),
        ),
    )
    complete_parts = (
        _part(
            "dp0-tp0",
            ParallelRank(dp=0, tp=0),
            0,
            topology_id=topology.topology_id,
        ),
        _part(
            "dp0-tp1",
            ParallelRank(dp=0, tp=1),
            4,
            topology_id=topology.topology_id,
        ),
    )
    empty_parts = tuple(
        WeightPlacementPart(
            resource_id=MODEL_ID,
            revision=REVISION,
            weight_generation=WEIGHT_GENERATION,
            placement_set_id=PLACEMENT_SET_ID,
            topology_id=topology.topology_id,
            participant_id=f"dp1-tp{tp}",
            rank=ParallelRank(dp=1, tp=tp),
            tensors=(),
            fragments=(),
        )
        for tp in range(2)
    )

    with pytest.raises(ValueError, match=r"not fully covered: .*dp=1"):
        _manifest(topology=topology, parts=complete_parts + empty_parts)


def test_global_placement_rejects_missing_or_duplicate_participants() -> None:
    left = _part("worker-0", ParallelRank(tp=0), 0)
    right = _part("worker-1", ParallelRank(tp=1), 4)

    with pytest.raises(ValueError, match="missing topology participant"):
        _manifest(parts=(left,))
    with pytest.raises(ValueError, match="duplicate placement participant"):
        _manifest(parts=(left, replace(right, participant_id="worker-0")))


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ({"resource_id": "other-model"}, "resource_id"),
        ({"revision": "other-revision"}, "revision"),
        ({"placement_set_id": "other-set"}, "placement_set_id"),
        ({"weight_generation": 8}, "weight_generation"),
        ({"topology_id": "sha256:" + "0" * 64}, "topology_id"),
    ],
)
def test_global_placement_rejects_parts_from_another_collection(
    replacement: dict[str, object], message: str
) -> None:
    left = _part("worker-0", ParallelRank(tp=0), 0)
    right = replace(_part("worker-1", ParallelRank(tp=1), 4), **replacement)

    with pytest.raises(ValueError, match=message):
        _manifest(parts=(left, right))


def test_global_placement_rejects_incomplete_tensor_coverage() -> None:
    left = _part("worker-0", ParallelRank(tp=0), 0)
    right = _part("worker-1", ParallelRank(tp=1), 4)
    incomplete = replace(
        right,
        fragments=(
            replace(
                right.fragments[0],
                local_shape=(2, 4),
                nbytes=16,
            ),
        ),
    )

    with pytest.raises(ValueError, match="not fully covered"):
        _manifest(parts=(left, incomplete))


def test_global_placement_accepts_complete_replicas_on_multiple_pp_owners() -> None:
    tensor = descriptor(
        tensor_id="lm_head.weight",
        global_shape=(8, 4),
        layer_id=None,
        expert_id=None,
    )
    participants = tuple(
        TopologyParticipant(
            f"pp{pp_rank}-tp{tp_rank}",
            ParallelRank(tp=tp_rank, pp=pp_rank),
        )
        for pp_rank in range(2)
        for tp_rank in range(2)
    )
    topology = parallel_topology(participants=participants)
    fragments = tuple(
        PlacementFragment(
            placement_fragment_id=f"pp{pp_rank}-tp{tp_rank}",
            tensor_id=tensor.tensor_id,
            global_offset=(tp_rank * 4, 0),
            local_shape=(4, 4),
            nbytes=32,
            rank=ParallelRank(tp=tp_rank, pp=pp_rank),
        )
        for pp_rank in range(2)
        for tp_rank in range(2)
    )

    placement = placement_manifest(
        topology=topology,
        tensors=(tensor,),
        fragments=fragments,
    )

    assert {fragment.rank.pp for fragment in placement.fragments} == {0, 1}


def test_global_placement_rejects_coverage_split_across_pp_owners() -> None:
    tensor = descriptor(
        tensor_id="lm_head.weight",
        global_shape=(8, 4),
        layer_id=None,
        expert_id=None,
    )
    participants = (
        TopologyParticipant("pp0-tp0", ParallelRank(tp=0, pp=0)),
        TopologyParticipant("pp1-tp1", ParallelRank(tp=1, pp=1)),
    )
    topology = parallel_topology(participants=participants)
    fragments = tuple(
        PlacementFragment(
            placement_fragment_id=participant.participant_id,
            tensor_id=tensor.tensor_id,
            global_offset=(participant.rank.tp * 4, 0),
            local_shape=(4, 4),
            nbytes=32,
            rank=participant.rank,
        )
        for participant in participants
    )

    with pytest.raises(ValueError, match="not fully covered"):
        placement_manifest(
            topology=topology,
            tensors=(tensor,),
            fragments=fragments,
        )


def test_global_placement_rejects_coverage_split_across_ep_owners() -> None:
    tensor = descriptor(
        tensor_id="experts.weight",
        global_shape=(8, 4),
        expert_id=None,
        parallel_axes=(
            TensorParallelAxis(kind="ep"),
            TensorParallelAxis(kind="tp", split_dim=0),
        ),
    )
    participants = (
        TopologyParticipant("ep0-tp0", ParallelRank(ep=0, tp=0)),
        TopologyParticipant("ep1-tp1", ParallelRank(ep=1, tp=1)),
    )
    topology = parallel_topology(participants=participants)
    fragments = tuple(
        PlacementFragment(
            placement_fragment_id=participant.participant_id,
            tensor_id=tensor.tensor_id,
            global_offset=(participant.rank.tp * 4, 0),
            local_shape=(4, 4),
            nbytes=32,
            rank=participant.rank,
        )
        for participant in participants
    )

    with pytest.raises(ValueError, match="not fully covered"):
        placement_manifest(
            topology=topology,
            tensors=(tensor,),
            fragments=fragments,
        )


def test_topology_rejects_out_of_range_rank_coordinates() -> None:
    with pytest.raises(ValueError, match="tp rank"):
        ParallelTopology(
            tp_size=2,
            pp_size=1,
            ep_size=1,
            dp_size=1,
            participants=(TopologyParticipant("worker-0", ParallelRank(tp=2)),),
        )


def _binding(
    placement: WeightPlacementManifest,
    participant_id: str,
    *,
    instance_id: str,
) -> WeightRuntimeBindingManifest:
    part = next(
        item for item in placement.parts if item.participant_id == participant_id
    )
    tensor_by_id = {tensor.tensor_id: tensor for tensor in placement.tensors}
    return WeightRuntimeBindingManifest(
        resource_id=placement.resource_id,
        revision=placement.revision,
        placement_id=placement.placement_id,
        placement_digest=placement.digest,
        participant_id=participant_id,
        instance_id=instance_id,
        generation=3,
        lease_id=f"lease-{participant_id}",
        fragments=tuple(
            RuntimeBindingFragment(
                placement_fragment_id=fragment.placement_fragment_id,
                fragment_id=f"runtime-{fragment.placement_fragment_id}",
                address=0x1000 + index * 0x100,
                nbytes=fragment.nbytes,
                worker_id=participant_id,
                endpoint=f"{participant_id}:12345",
                device="cuda:0",
                itemsize=tensor_by_id[fragment.tensor_id].itemsize,
                local_shape=fragment.local_shape,
                strides_bytes=_contiguous_strides_bytes(
                    fragment.local_shape,
                    tensor_by_id[fragment.tensor_id].itemsize,
                ),
                storage_address=0x1000 + index * 0x100,
                storage_nbytes=fragment.nbytes,
                storage_offset_bytes=0,
            )
            for index, fragment in enumerate(part.fragments)
        ),
    )


def test_runtime_bindings_bind_each_part_of_one_global_placement() -> None:
    placement = _manifest()
    bindings = (
        _binding(placement, "worker-0", instance_id="source-0"),
        _binding(placement, "worker-1", instance_id="source-1"),
    )

    assert validate_runtime_binding(placement, bindings[0]) is None
    assert validate_runtime_bindings(placement, bindings) is None


def test_runtime_binding_set_rejects_cross_participant_address_overlap() -> None:
    placement = _manifest()
    left = _binding(placement, "worker-0", instance_id="shared-instance")
    right = _binding(placement, "worker-1", instance_id="shared-instance")
    left_fragment = left.fragments[0]
    right_fragment = replace(
        right.fragments[0],
        address=left_fragment.address,
        worker_id=left_fragment.worker_id,
        endpoint=left_fragment.endpoint,
        device=left_fragment.device,
        storage_address=left_fragment.storage_address,
        storage_nbytes=left_fragment.storage_nbytes,
        storage_offset_bytes=left_fragment.storage_offset_bytes,
    )

    with pytest.raises(ValueError, match="address ranges overlap"):
        validate_runtime_bindings(
            placement,
            (left, replace(right, fragments=(right_fragment,))),
        )


def test_runtime_binding_set_requires_every_global_part_exactly_once() -> None:
    placement = _manifest()
    left = _binding(placement, "worker-0", instance_id="source-0")
    right = _binding(placement, "worker-1", instance_id="source-1")

    with pytest.raises(ValueError, match="missing runtime binding participant"):
        validate_runtime_bindings(placement, (left,))
    with pytest.raises(ValueError, match="duplicate runtime binding participant"):
        validate_runtime_bindings(
            placement,
            (left, replace(right, participant_id="worker-0")),
        )
    with pytest.raises(ValueError, match="unknown runtime binding participant"):
        validate_runtime_bindings(
            placement,
            (left, right, replace(right, participant_id="worker-unknown")),
        )


def test_runtime_binding_set_does_not_require_empty_participants() -> None:
    topology = ParallelTopology(
        tp_size=2,
        pp_size=2,
        ep_size=1,
        dp_size=1,
        participants=(
            TopologyParticipant("worker-0", ParallelRank(tp=0, pp=0)),
            TopologyParticipant("worker-1", ParallelRank(tp=1, pp=0)),
            TopologyParticipant("worker-empty", ParallelRank(tp=0, pp=1)),
        ),
    )
    placement = _manifest(
        topology=topology,
        parts=(
            _part(
                "worker-0",
                ParallelRank(tp=0, pp=0),
                0,
                topology_id=topology.topology_id,
            ),
            _part(
                "worker-1",
                ParallelRank(tp=1, pp=0),
                4,
                topology_id=topology.topology_id,
            ),
            WeightPlacementPart(
                resource_id=MODEL_ID,
                revision=REVISION,
                weight_generation=WEIGHT_GENERATION,
                placement_set_id=PLACEMENT_SET_ID,
                topology_id=topology.topology_id,
                participant_id="worker-empty",
                rank=ParallelRank(tp=0, pp=1),
                tensors=(),
                fragments=(),
            ),
        ),
    )
    bindings = (
        _binding(placement, "worker-0", instance_id="source-0"),
        _binding(placement, "worker-1", instance_id="source-1"),
    )

    assert WeightPlacementManifest.from_json(placement.to_json()) == placement
    assert validate_runtime_bindings(placement, bindings) is None


def test_runtime_binding_can_only_bind_its_declared_part() -> None:
    placement = _manifest()
    left = _binding(placement, "worker-0", instance_id="source-0")
    right_part = next(
        item for item in placement.parts if item.participant_id == "worker-1"
    )
    wrong_fragment = replace(
        left.fragments[0],
        placement_fragment_id=right_part.fragments[0].placement_fragment_id,
    )

    with pytest.raises(ValueError, match="unknown placement fragment"):
        validate_runtime_binding(
            placement,
            replace(left, fragments=(wrong_fragment,)),
        )
