from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from math import prod

import pytest
from mooncake.reshard.contracts import (
    ParticipantId,
    PlacementSetId,
    ResourceId,
    RevisionId,
    RuntimeBindingFragment,
    RuntimeFragmentId,
    RuntimeInstanceId,
)
from mooncake.reshard.kv_cache import (
    KVCacheBufferBinding,
    KVCacheComponent,
    KVCacheDescriptor,
    KVCacheLogicalTransferPlan,
    KVCachePlacementManifest,
    KVCachePlacementPart,
    KVCacheRank,
    KVCacheRuntimeBindingManifest,
    KVCacheRuntimeBuffer,
    KVCacheSnapshotDescriptor,
    KVCacheTopology,
    KVCacheTopologyParticipant,
    KVCacheTransferEdge,
    assemble_kv_cache_placement,
    kv_cache_logical_plan_from_json,
    kv_cache_logical_plan_to_json,
    kv_cache_part_from_json,
    kv_cache_part_to_json,
    kv_cache_placement_from_json,
    kv_cache_placement_to_json,
    kv_cache_runtime_binding_from_json,
    kv_cache_runtime_binding_to_json,
    kv_cache_snapshot_from_json,
    kv_cache_snapshot_to_json,
    placement_fragment_id,
    plan_kv_cache_transfer_to_local_target,
    prepare_kv_cache_transfer,
    validate_runtime_binding,
    validate_runtime_bindings,
)


_MAX_U64 = (1 << 64) - 1


def _placement(
    prefix: str,
    layer_partitions: tuple[tuple[int, ...], ...],
    tp_size: int,
    *,
    total_kv_heads: int = 4,
    page_size: int = 16,
    dp_size: int = 1,
    resource_id: str = "kv:qwen-test",
    revision: str = "qwen-test-revision",
) -> KVCachePlacementManifest:
    descriptor = KVCacheDescriptor(
        global_layer_ids=tuple(
            sorted(layer for partition in layer_partitions for layer in partition)
        ),
        dtype="float16",
        itemsize=2,
        page_size=page_size,
        total_kv_heads=total_kv_heads,
        key_head_dim=8,
        value_head_dim=8,
    )
    participants = tuple(
        KVCacheTopologyParticipant(
            participant_id=ParticipantId(
                f"{prefix}-d{dp_rank}-p{pp_rank}-t{tp_rank}"
                if dp_size > 1
                else f"{prefix}-p{pp_rank}-t{tp_rank}"
            ),
            rank=KVCacheRank(dp=dp_rank, pp=pp_rank, tp=tp_rank),
        )
        for dp_rank in range(dp_size)
        for pp_rank in range(len(layer_partitions))
        for tp_rank in range(tp_size)
    )
    topology = KVCacheTopology(
        dp_size=dp_size,
        pp_size=len(layer_partitions),
        tp_size=tp_size,
        participants=participants,
    )
    parts = []
    for participant in topology.participants:
        pp_rank = participant.rank.pp
        tp_rank = participant.rank.tp
        if total_kv_heads >= tp_size and total_kv_heads % tp_size == 0:
            head_count = total_kv_heads // tp_size
            head_start = tp_rank * head_count
            replica_count = 1
            replica_ordinal = 0
        elif tp_size > total_kv_heads and tp_size % total_kv_heads == 0:
            replica_count = tp_size // total_kv_heads
            head_count = 1
            head_start = tp_rank // replica_count
            replica_ordinal = tp_rank % replica_count
        else:
            raise ValueError("test topology has an unsupported TP/head ratio")
        parts.append(
            KVCachePlacementPart(
                resource_id=ResourceId(resource_id),
                revision=RevisionId(revision),
                placement_set_id=PlacementSetId(f"{prefix}-placement"),
                topology_id=topology.topology_id,
                participant_id=participant.participant_id,
                rank=participant.rank,
                descriptor=descriptor,
                layer_ids=layer_partitions[pp_rank],
                head_start=head_start,
                head_count=head_count,
                replica_ordinal=replica_ordinal,
                replica_count=replica_count,
            )
        )
    return KVCachePlacementManifest(
        resource_id=ResourceId(resource_id),
        revision=RevisionId(revision),
        placement_set_id=PlacementSetId(f"{prefix}-placement"),
        topology=topology,
        descriptor=descriptor,
        parts=tuple(parts),
    )


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _snapshot(
    *,
    resource_id: str = "kv:qwen-test",
    token_start: int = 0,
    token_count: int = 8,
) -> KVCacheSnapshotDescriptor:
    return KVCacheSnapshotDescriptor(
        namespace="tenant-a",
        resource_id=ResourceId(resource_id),
        model_id="Qwen/Qwen2.5-1.5B-Instruct",
        model_revision="main",
        token_start=token_start,
        token_count=token_count,
        token_fingerprint=_fingerprint("token-ids"),
        semantic_fingerprint=_fingerprint("model-and-adapter"),
    )


def _binding(
    placement: KVCachePlacementManifest,
    participant_id: str,
    *,
    base_address: int,
    snapshot: KVCacheSnapshotDescriptor | None = None,
    capacity_tokens: int = 256,
) -> KVCacheRuntimeBindingManifest:
    typed_participant_id = ParticipantId(participant_id)
    part = placement.part(typed_participant_id)
    buffers = []
    for buffer_index, (layer_id, component) in enumerate(
        (layer_id, component)
        for layer_id in part.layer_ids
        for component in KVCacheComponent
    ):
        head_dim = (
            placement.descriptor.key_head_dim
            if component is KVCacheComponent.KEY
            else placement.descriptor.value_head_dim
        )
        shape = (capacity_tokens, part.head_count, head_dim)
        nbytes = prod(shape) * placement.descriptor.itemsize
        address = base_address + buffer_index * 1_000_000
        fragment = KVCacheRuntimeBuffer(
            placement_fragment_id=placement_fragment_id(
                typed_participant_id,
                layer_id,
                component,
                head_start=part.head_start,
                head_count=part.head_count,
            ),
            fragment_id=RuntimeFragmentId(
                f"runtime:{participant_id}:{layer_id}:{component.value}"
            ),
            address=address,
            nbytes=nbytes,
            worker_id=participant_id,
            endpoint=f"{participant_id}:12345",
            device="cuda:0",
            itemsize=placement.descriptor.itemsize,
            local_shape=shape,
            strides_bytes=(
                part.head_count * head_dim * placement.descriptor.itemsize,
                head_dim * placement.descriptor.itemsize,
                placement.descriptor.itemsize,
            ),
            storage_address=address,
            storage_nbytes=nbytes,
            storage_offset_bytes=0,
        )
        buffers.append(KVCacheBufferBinding(layer_id, component, fragment))
    return KVCacheRuntimeBindingManifest(
        resource_id=placement.resource_id,
        placement_id=placement.placement_id,
        placement_digest=placement.digest,
        instance_id=RuntimeInstanceId(f"instance:{participant_id}"),
        snapshot_id=snapshot.snapshot_id if snapshot is not None else None,
        snapshot_digest=snapshot.digest if snapshot is not None else None,
        revision=placement.revision,
        participant_id=typed_participant_id,
        buffers=tuple(buffers),
    )


def _binding_with_address_space(
    binding, *, instance="shared", worker="worker", device="cuda:0"
):
    return replace(
        binding,
        instance_id=RuntimeInstanceId(instance),
        buffers=tuple(
            replace(
                item, fragment=replace(item.fragment, worker_id=worker, device=device)
            )
            for item in binding.buffers
        ),
    )


def _buffer_with_storage(item, address, storage_address, storage_nbytes):
    return replace(
        item,
        fragment=replace(
            item.fragment,
            address=address,
            storage_address=storage_address,
            storage_nbytes=storage_nbytes,
            storage_offset_bytes=address - storage_address,
        ),
    )


def _plan(
    source: KVCachePlacementManifest,
    target: KVCachePlacementManifest,
    participant_id: str,
    *,
    source_dp_rank: int | None = None,
    snapshot: KVCacheSnapshotDescriptor | None = None,
) -> KVCacheLogicalTransferPlan:
    return plan_kv_cache_transfer_to_local_target(
        source,
        target,
        ParticipantId(participant_id),
        source_dp_rank=source_dp_rank,
        snapshot=snapshot,
    )


def test_placement_part_and_binding_round_trip_are_canonical() -> None:
    placement = _placement("source", ((0, 1), (2, 3)), 2)
    binding = _binding(placement, "source-p0-t0", base_address=1_000_000_000)
    restored = kv_cache_placement_from_json(kv_cache_placement_to_json(placement))
    restored_part = kv_cache_part_from_json(kv_cache_part_to_json(placement.parts[0]))
    restored_binding = kv_cache_runtime_binding_from_json(
        kv_cache_runtime_binding_to_json(binding)
    )

    assert KVCacheRuntimeBuffer is RuntimeBindingFragment
    assert restored == placement
    assert restored_part == placement.parts[0]
    assert restored_binding == binding
    part_payload = json.loads(kv_cache_part_to_json(placement.parts[0]))
    assert "placement_id" not in part_payload
    assert "instance_id" not in part_payload
    assert "address" not in json.dumps(part_payload)


@pytest.mark.parametrize(
    "heads,dim,itemsize,accepted",
    [
        (1, 1 << 63, 2, False),
        (2, 1 << 62, 2, False),
        (_MAX_U64, 1, 1, True),
        (1, _MAX_U64, 1, True),
        (1, (1 << 63) - 1, 2, True),
    ],
)
def test_transfer_edge_byte_size_boundary(heads, dim, itemsize, accepted):
    args = dict(
        source_participant_id="s",
        target_participant_id="t",
        global_layer_id=0,
        component=KVCacheComponent.KEY,
        global_head_start=0,
        head_count=heads,
        source_head_offset=0,
        target_head_offset=0,
        head_dim=dim,
        itemsize=itemsize,
    )
    if accepted:
        assert KVCacheTransferEdge(**args).inner_bytes == heads * dim * itemsize
    else:
        with pytest.raises(ValueError, match="inner_bytes"):
            KVCacheTransferEdge(**args)


@pytest.mark.parametrize("component", ["key_head_dim", "value_head_dim"])
def test_descriptor_rejects_overflowing_head_bytes(component):
    descriptor = _placement("source", ((0,),), 1).descriptor
    with pytest.raises(ValueError, match="byte size"):
        replace(descriptor, **{component: 1 << 63})


def test_descriptor_keeps_distributed_global_size_separate_from_local_size():
    descriptor = KVCacheDescriptor(
        global_layer_ids=(0,),
        dtype="uint8",
        itemsize=1,
        page_size=1,
        total_kv_heads=2,
        key_head_dim=_MAX_U64,
        value_head_dim=1,
    )
    assert descriptor.total_kv_heads * descriptor.key_head_dim > _MAX_U64


@pytest.mark.parametrize(
    ("source_tp", "target_tp", "target_rank", "expected_intervals"),
    [
        (1, 2, 1, ((2, 2),)),
        (2, 1, 0, ((0, 2), (2, 2))),
        (1, 4, 3, ((3, 1),)),
        (4, 1, 0, ((0, 1), (1, 1), (2, 1), (3, 1))),
    ],
)
def test_tp_scatter_and_gather(
    source_tp: int,
    target_tp: int,
    target_rank: int,
    expected_intervals: tuple[tuple[int, int], ...],
) -> None:
    source = _placement("source", ((0,),), source_tp)
    target = _placement("target", ((0,),), target_tp)
    plan = _plan(source, target, f"target-p0-t{target_rank}")

    key_intervals = tuple(
        (edge.global_head_start, edge.head_count)
        for edge in plan.edges
        if edge.component is KVCacheComponent.KEY
    )
    assert key_intervals == expected_intervals


def test_huge_head_count_plans_by_intervals():
    source = _placement("source", ((0,),), 1, total_kv_heads=1 << 40)
    target = _placement("target", ((0,),), 2, total_kv_heads=1 << 40)
    plan = plan_kv_cache_transfer_to_local_target(
        source, target, target.parts[0].participant_id
    )
    assert len(plan.edges) == 2


def test_gqa_replica_ordinal_selects_one_source_writer() -> None:
    source = _placement("source", ((0,),), 4, total_kv_heads=2)
    target = _placement("target", ((0,),), 4, total_kv_heads=2)
    plan = _plan(source, target, "target-p0-t3")

    assert plan.source_participant_ids == (ParticipantId("source-p0-t3"),)
    assert {edge.global_head_start for edge in plan.edges} == {1}


def test_prepare_attests_both_complete_global_placements() -> None:
    source = _placement("source", ((0,),), 2)
    target = _placement("target", ((0,),), 1)
    full_plan = _plan(source, target, "target-p0-t0")
    source_id = full_plan.source_participant_ids[0]
    plan = full_plan.for_source(source_id)
    source_binding = _binding(source, source_id, base_address=1_000_000_000)
    target_binding = _binding(target, "target-p0-t0", base_address=2_000_000_000)

    prepared = prepare_kv_cache_transfer(plan, source_binding, target_binding)

    assert prepared.source_placement_id == source.placement_id
    assert prepared.target_placement_id == target.placement_id
    assert prepared.source_placement_digest == source.digest
    assert prepared.target_placement_digest == target.digest

    stale = replace(target_binding, placement_id=source.placement_id)
    with pytest.raises(ValueError, match="placement_id"):
        prepare_kv_cache_transfer(plan, source_binding, stale)


def test_arbitrary_pp_intersections_choose_exact_participants() -> None:
    source = _placement("source", (tuple(range(14)), tuple(range(14, 28))), 1)
    target = _placement(
        "target", (tuple(range(10)), tuple(range(10, 19)), tuple(range(19, 28))), 1
    )
    plan = _plan(source, target, "target-p1-t0")

    assert plan.source_participant_ids == (
        ParticipantId("source-p0-t0"),
        ParticipantId("source-p1-t0"),
    )
    assert len(plan.edges) == 18


def test_arbitrary_dp_pp_tp_topologies_select_one_source_replica() -> None:
    source = _placement(
        "source",
        ((0, 1), (2, 3), (4, 5)),
        2,
        dp_size=2,
    )
    target = _placement(
        "target",
        ((0, 1, 2), (3, 4, 5)),
        4,
        dp_size=4,
    )

    for target_dp_rank in range(4):
        participant_id = f"target-d{target_dp_rank}-p0-t3"
        plan = _plan(source, target, participant_id)
        assert plan.source_dp_rank == target_dp_rank % 2
        assert {
            source.part(participant_id).rank.dp
            for participant_id in plan.source_participant_ids
        } == {target_dp_rank % 2}

    explicit = _plan(
        source,
        target,
        "target-d3-p0-t3",
        source_dp_rank=0,
    )
    assert explicit.source_dp_rank == 0
    assert explicit.source_participant_ids == (
        ParticipantId("source-d0-p0-t1"),
        ParticipantId("source-d0-p1-t1"),
    )

    with pytest.raises(ValueError, match="source DP rank 9"):
        _plan(
            source,
            target,
            "target-d0-p0-t0",
            source_dp_rank=9,
        )


def test_role_agnostic_placement_assembly_supports_multiple_dp_replicas() -> None:
    placement = _placement("server-a", ((0,), (1,)), 2, dp_size=3)
    assembled = assemble_kv_cache_placement(
        placement.parts,
        dp_size=3,
        pp_size=2,
        tp_size=2,
    )
    assert assembled == placement
    assert assembled.dp_ranks == (0, 1, 2)


def test_complete_placement_rejects_missing_participant_and_coverage() -> None:
    complete = _placement("source", ((0,),), 2)
    with pytest.raises(ValueError, match="missing topology participant"):
        KVCachePlacementManifest(
            resource_id=complete.resource_id,
            revision=complete.revision,
            placement_set_id=complete.placement_set_id,
            topology=complete.topology,
            descriptor=complete.descriptor,
            parts=complete.parts[:1],
        )

    malformed = replace(complete.parts[1], head_start=1, head_count=1)
    with pytest.raises(ValueError, match="overlapping|misses"):
        KVCachePlacementManifest(
            resource_id=complete.resource_id,
            revision=complete.revision,
            placement_set_id=complete.placement_set_id,
            topology=complete.topology,
            descriptor=complete.descriptor,
            parts=(complete.parts[0], malformed),
        )


def test_binding_requires_exact_participant_membership_and_global_digest() -> None:
    placement = _placement("source", ((0,),), 2)
    bindings = tuple(
        _binding(
            placement, part.participant_id, base_address=1_000_000_000 + i * 10_000_000
        )
        for i, part in enumerate(placement.parts)
    )
    validate_runtime_bindings(placement, bindings)
    validate_runtime_binding(placement, bindings[0])

    with pytest.raises(ValueError, match="missing runtime binding participant"):
        validate_runtime_bindings(placement, bindings[:1])
    with pytest.raises(ValueError, match="digest"):
        validate_runtime_binding(
            placement,
            replace(bindings[0], placement_digest="0" * 64),
        )


@pytest.mark.parametrize("different", ["worker", "device", "instance", None])
def test_identical_virtual_addresses_respect_address_space(different):
    placement = _placement("source", ((0,),), 2)
    first = _binding_with_address_space(
        _binding(
            placement,
            placement.parts[0].participant_id,
            base_address=1_000_000,
            capacity_tokens=1,
        )
    )
    second = _binding_with_address_space(
        _binding(
            placement,
            placement.parts[1].participant_id,
            base_address=1_000_000,
            capacity_tokens=1,
        ),
        **({different: "other"} if different else {}),
    )
    if different:
        validate_runtime_bindings(placement, (first, second))
    else:
        with pytest.raises(ValueError, match="overlap"):
            validate_runtime_bindings(placement, (first, second))


def test_per_participant_views_also_respect_device_address_space():
    placement = _placement("source", ((0,),), 1)
    binding = _binding(
        placement,
        placement.parts[0].participant_id,
        base_address=1_000_000,
        capacity_tokens=1,
    )
    first, second = binding.buffers
    second = replace(
        second,
        fragment=replace(
            second.fragment,
            address=first.fragment.address,
            storage_address=first.fragment.storage_address,
            device="cuda:1",
        ),
    )
    validate_runtime_binding(placement, replace(binding, buffers=(first, second)))


@pytest.mark.parametrize(
    "allocations,accepted",
    [
        (((1024, 256), (1152, 256)), False),
        (((1024, 512), (1024, 512)), True),
        (((1024, 256), (1280, 256)), True),
        (((1024, 256), (1024, 512)), False),
    ],
)
def test_backing_allocations_with_disjoint_views(allocations, accepted):
    placement = _placement("source", ((0,),), 1)
    binding = _binding(
        placement,
        placement.parts[0].participant_id,
        base_address=1_000_000,
        capacity_tokens=1,
    )
    buffers = tuple(
        _buffer_with_storage(item, address, *allocation)
        for item, address, allocation in zip(binding.buffers, (1024, 1280), allocations)
    )
    binding = replace(binding, buffers=buffers)
    if accepted:
        validate_runtime_binding(placement, binding)
    else:
        with pytest.raises(ValueError, match="backing allocation.*overlap"):
            validate_runtime_binding(placement, binding)


@pytest.mark.parametrize("same_allocation", [True, False])
def test_backing_allocations_across_participants(same_allocation):
    placement = _placement("source", ((0,),), 2)
    first = _binding_with_address_space(
        _binding(
            placement,
            placement.parts[0].participant_id,
            base_address=1_000_000,
            capacity_tokens=1,
        )
    )
    second = _binding_with_address_space(
        _binding(
            placement,
            placement.parts[1].participant_id,
            base_address=3_000_000,
            capacity_tokens=1,
        )
    )
    first = replace(
        first,
        buffers=(
            _buffer_with_storage(
                first.buffers[0], 1024, 1024, 512 if same_allocation else 256
            ),
            first.buffers[1],
        ),
    )
    second = replace(
        second,
        buffers=(
            _buffer_with_storage(
                second.buffers[0],
                1280,
                1024 if same_allocation else 1152,
                512 if same_allocation else 256,
            ),
            second.buffers[1],
        ),
    )
    if same_allocation:
        validate_runtime_bindings(placement, (first, second))
    else:
        with pytest.raises(ValueError, match="backing allocation.*overlap"):
            validate_runtime_bindings(placement, (first, second))


def test_shared_backing_allocation_does_not_allow_overlapping_views():
    placement = _placement("source", ((0,),), 1)
    binding = _binding(
        placement,
        placement.parts[0].participant_id,
        base_address=1_000_000,
        capacity_tokens=1,
    )
    binding = replace(
        binding,
        buffers=tuple(
            _buffer_with_storage(item, 1024, 1024, 512) for item in binding.buffers
        ),
    )
    with pytest.raises(ValueError, match="buffers overlap"):
        validate_runtime_binding(placement, binding)


def test_singleton_stride_is_normalized_before_binding_validation() -> None:
    placement = _placement("source", ((0,),), 4)
    binding = _binding(placement, "source-p0-t0", base_address=1_000_000_000)
    first = binding.buffers[0]
    fragment = first.fragment
    assert fragment.local_shape[1] == 1
    noncanonical = replace(
        fragment,
        strides_bytes=(fragment.strides_bytes[0], 999_999, fragment.strides_bytes[2]),
    )
    normalized = replace(
        binding,
        buffers=(
            replace(first, fragment=noncanonical),
            *binding.buffers[1:],
        ),
    )
    validate_runtime_binding(placement, normalized)


def test_logical_plan_round_trip_preserves_both_global_placements() -> None:
    source = _placement("source", ((0,),), 2)
    target = _placement("target", ((0,),), 1)
    plan = _plan(source, target, "target-p0-t0").for_source(
        ParticipantId("source-p0-t0")
    )

    restored = kv_cache_logical_plan_from_json(kv_cache_logical_plan_to_json(plan))

    assert restored == plan
    assert restored.source_placement.placement_id == source.placement_id
    assert restored.target_placement.placement_id == target.placement_id

    duplicate = kv_cache_logical_plan_to_json(plan).replace(
        '"expected_writer_ids":',
        '"expected_writer_ids":[],"expected_writer_ids":',
        1,
    )
    with pytest.raises(ValueError, match="duplicate JSON field"):
        kv_cache_logical_plan_from_json(duplicate)


def test_snapshot_contract_is_canonical_and_optional_for_pd() -> None:
    snapshot = _snapshot(token_start=4, token_count=6)
    restored = kv_cache_snapshot_from_json(kv_cache_snapshot_to_json(snapshot))

    assert restored == snapshot
    assert restored.token_end == 10

    source = _placement("source", ((0,),), 1)
    semantic_target = _placement("semantic-target", ((0,),), 1)
    plain = _plan(source, semantic_target, "semantic-target-p0-t0")
    semantic = _plan(
        source,
        semantic_target,
        "semantic-target-p0-t0",
        snapshot=snapshot,
    )
    assert plain.plan_id == semantic.plan_id
    assert plain.digest != semantic.digest

    target = _placement(
        "target",
        ((0,),),
        1,
        resource_id="kv:framework-owned-target",
        revision="different-framework-revision",
    )
    plan = _plan(source, target, "target-p0-t0")
    assert plan.snapshot is None

    with pytest.raises(ValueError, match="target placement resource_id"):
        _plan(source, target, "target-p0-t0", snapshot=snapshot)


@pytest.mark.parametrize(
    "start,count,accepted",
    [(_MAX_U64, 1, False), (_MAX_U64 - 1, 1, True), (0, _MAX_U64, True)],
)
def test_snapshot_token_end_boundary(start, count, accepted):
    if accepted:
        assert (
            _snapshot(token_start=start, token_count=count).token_end == start + count
        )
    else:
        with pytest.raises(ValueError, match="token_end"):
            _snapshot(token_start=start, token_count=count)


def test_page_size_repacking_is_rejected_explicitly() -> None:
    source = _placement("source", ((0,),), 1, page_size=16)
    target = _placement("target", ((0,),), 1, page_size=32)

    with pytest.raises(ValueError, match="page_size.*unsupported"):
        _plan(source, target, "target-p0-t0")


def test_logical_plan_identity_and_edges_are_semantically_verified() -> None:
    source = _placement("source", ((0,),), 2)
    target = _placement("target", ((0,),), 1)
    plan = _plan(source, target, "target-p0-t0")
    restored = kv_cache_logical_plan_from_json(kv_cache_logical_plan_to_json(plan))

    assert restored.plan_id == plan.plan_id
    assert restored.digest == plan.digest

    malformed_edge = replace(plan.edges[0], head_count=plan.edges[0].head_count + 1)
    with pytest.raises(ValueError, match="canonical plan"):
        KVCacheLogicalTransferPlan(
            source_placement=source,
            target_placement=target,
            target_participant_id=plan.target_participant_id,
            edges=(malformed_edge, *plan.edges[1:]),
            expected_writer_ids=plan.expected_writer_ids,
        )


def test_snapshot_bound_runtime_bindings_are_checked_at_prepare() -> None:
    snapshot = _snapshot()
    source = _placement("source", ((0,),), 2)
    target = _placement("target", ((0,),), 1)
    full_plan = _plan(source, target, "target-p0-t0", snapshot=snapshot)
    source_id = full_plan.source_participant_ids[0]
    plan = full_plan.for_source(source_id)
    source_binding = _binding(
        source,
        source_id,
        base_address=1_000_000_000,
        snapshot=snapshot,
    )
    target_binding = _binding(
        target,
        "target-p0-t0",
        base_address=2_000_000_000,
        snapshot=snapshot,
    )

    prepared = prepare_kv_cache_transfer(plan, source_binding, target_binding)
    assert prepared.snapshot_id == snapshot.snapshot_id
    assert prepared.snapshot_digest == snapshot.digest

    binding_payload = json.loads(kv_cache_runtime_binding_to_json(source_binding))
    assert "generation" not in binding_payload
    assert "lease_id" not in binding_payload

    stale = replace(target_binding, snapshot_digest="0" * 64)
    with pytest.raises(ValueError, match="snapshot digest"):
        prepare_kv_cache_transfer(plan, source_binding, stale)


@pytest.mark.parametrize(
    "source_snapshot,target_snapshot,explicit_plan,mismatched_digest,accepted",
    [
        (None, None, False, False, True),
        (0, 0, False, False, True),
        (0, 8, False, False, False),
        (0, None, False, False, False),
        (None, 0, False, False, False),
        (0, 0, True, False, True),
        (0, 8, True, False, False),
        (None, None, True, False, False),
        (0, 0, False, True, False),
    ],
)
def test_prepare_snapshot_identity(
    source_snapshot, target_snapshot, explicit_plan, mismatched_digest, accepted
):
    source = _placement("source", ((0,),), 1)
    target = _placement("target", ((0,),), 1)
    snapshot = _snapshot()
    sb = _binding(
        source,
        source.parts[0].participant_id,
        base_address=1_000_000,
        snapshot=_snapshot(token_start=source_snapshot)
        if source_snapshot is not None
        else None,
    )
    tb = _binding(
        target,
        target.parts[0].participant_id,
        base_address=3_000_000,
        snapshot=_snapshot(token_start=target_snapshot)
        if target_snapshot is not None
        else None,
    )
    if mismatched_digest:
        tb = replace(tb, snapshot_digest="0" * 64)
    plan = _plan(
        source,
        target,
        target.parts[0].participant_id,
        snapshot=snapshot if explicit_plan else None,
    )
    if not accepted:
        with pytest.raises(ValueError, match="snapshot"):
            prepare_kv_cache_transfer(plan, sb, tb)
    else:
        prepared = prepare_kv_cache_transfer(plan, sb, tb)
        assert prepared.snapshot_id == sb.snapshot_id
        assert prepared.snapshot_digest == sb.snapshot_digest
