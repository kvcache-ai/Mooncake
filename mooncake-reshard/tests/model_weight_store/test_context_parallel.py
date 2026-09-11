from __future__ import annotations

import ctypes
import json
from dataclasses import replace
from itertools import product
from math import prod
from pathlib import Path

import pytest

from mooncake.reshard.weight.manifest import (
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
)
from mooncake.reshard.weight.serde import (
    weight_placement_from_json,
    weight_placement_to_json,
)

from .helpers import (
    RuntimeParticipant,
    bound_fragments,
    expected_multi_dim_fragment,
    load_all,
    make_runtime_inputs,
    make_weight_store,
    plan_transfer,
)
from .test_snapshot_writer import _SnapshotAdapter, _snapshot_descriptor


def cp_inputs(
    side,
    *,
    cp=2,
    tp=2,
    cp_dim=1,
    tp_dim=2,
    ep=1,
    dp=1,
    pp=1,
    cp_semantics="split",
    cp_ranges=None,
):
    axes = [ReplicatedAxis("dp"), OwnershipAxis("pp"), SplitAxis("ep", 0)]
    if cp_semantics == "split":
        axes.append(SplitAxis("cp", cp_dim))
    elif cp_semantics == "replicated":
        axes.append(ReplicatedAxis("cp"))
    else:
        axes.append(OwnershipAxis("cp"))
    axes.append(SplitAxis("tp", tp_dim))
    tensor = TensorDescriptor(
        tensor_id="tensor",
        global_shape=(4, 6, 8),
        dtype="uint8",
        itemsize=1,
        shard_dims=tuple(
            sorted(axis.dim for axis in axes if isinstance(axis, SplitAxis))
        ),
        parallel_axes=tuple(axes),
        layout_fingerprint="logical-contiguous-u8",
    )
    participants = []
    for d, p, e, t, c in product(range(dp), range(pp), range(ep), range(tp), range(cp)):
        rank = ParallelRank(dp=d, pp=p, ep=e, tp=t, cp=c)
        shape, offset = list(tensor.global_shape), [0, 0, 0]
        for axis in axes:
            if isinstance(axis, SplitAxis):
                size = {"ep": ep, "tp": tp, "cp": cp}[axis.kind]
                shape[axis.dim] //= size
                offset[axis.dim] = getattr(rank, axis.kind) * shape[axis.dim]
        worker = f"{side}-d{d}-p{p}-e{e}-t{t}-c{c}"
        placement_fragments, binding_fragments = [], []
        regions = (
            cp_ranges[c] if cp_ranges is not None else [(offset[cp_dim], shape[cp_dim])]
        )
        for region_index, (begin, extent) in enumerate(regions):
            offset[cp_dim], shape[cp_dim] = begin, extent
            fragment = PlacementFragment(
                tensor_id=tensor.tensor_id,
                global_offset=tuple(offset),
                local_shape=tuple(shape),
                nbytes=prod(shape),
                rank=rank,
            )
            payload = (
                expected_multi_dim_fragment(fragment)
                if side == "source"
                else bytes([255]) * prod(shape)
            )
            owner = (ctypes.c_ubyte * len(payload))(*payload)
            runtime = RuntimeBindingFragment(
                placement_fragment_id=fragment.fragment_id,
                fragment_id=f"{worker}-runtime-{region_index}",
                address=ctypes.addressof(owner),
                nbytes=len(payload),
                worker_id=worker,
                endpoint=f"{worker}:12345",
                device="cpu",
                itemsize=1,
                local_shape=tuple(shape),
                strides_bytes=(shape[1] * shape[2], shape[2], 1),
                storage_address=ctypes.addressof(owner),
                storage_nbytes=len(payload),
                storage_offset_bytes=0,
                owner=owner,
            )
            placement_fragments.append(fragment)
            binding_fragments.append(runtime)
        participants.append(
            RuntimeParticipant(
                participant_id=worker,
                rank=rank,
                instance_id=worker,
                placement_fragments=tuple(placement_fragments),
                binding_fragments=tuple(binding_fragments),
            )
        )
    return make_runtime_inputs(
        resource_id="cp-model",
        revision="revision",
        placement_set_id=side,
        tensors=(tensor,),
        participants=tuple(participants),
    )


@pytest.mark.parametrize(
    "source_config,target_config",
    [
        ({}, {"cp": 3, "tp": 4}),
        (
            {"cp_ranges": (((0, 1), (2, 1), (4, 1)), ((1, 1), (3, 1), (5, 1)))},
            {"cp": 3},
        ),
        ({"cp_ranges": (((0, 2),), ((2, 4),))}, {"cp": 3}),
        (
            {"cp": 3},
            {"cp_ranges": (((0, 1), (2, 1), (4, 1)), ((1, 1), (3, 1), (5, 1)))},
        ),
        ({}, {"cp": 4, "tp": 3, "cp_dim": 2, "tp_dim": 1}),
        ({"cp": 3, "tp": 4}, {"cp": 1, "tp": 1}),
        ({"cp": 1, "tp": 1}, {"cp": 3, "tp": 4}),
        ({"ep": 2, "dp": 2, "pp": 2}, {"ep": 4, "dp": 2, "pp": 2, "cp": 3}),
        ({"cp_semantics": "replicated", "ep": 2, "dp": 2}, {"cp": 3, "ep": 4}),
        ({}, {"cp_semantics": "replicated", "cp": 3}),
        ({"cp_semantics": "replicated"}, {"cp_semantics": "replicated", "cp": 3}),
        ({"cp_semantics": "ownership"}, {"cp_semantics": "ownership", "cp": 3}),
    ],
)
def test_cp_snapshot_restore_exact_bytes(source_config, target_config):
    sources = cp_inputs("source", **source_config)
    targets = cp_inputs("target", **target_config)

    # Both runtime-to-runtime planning/binding and persisted-source restore must
    # retain every CP target, including replicas with identical logical boxes.
    runtime_plan = plan_transfer(sources, targets)
    assert {op.target.rank.cp for op in runtime_plan.operations} == set(
        range(targets.placement.topology.cp_size)
    )
    store, weight_store = make_weight_store(max_range_bytes=7, max_ranges_per_request=3)
    writer = weight_store.begin_weight_snapshot(
        _snapshot_descriptor(sources),
        _SnapshotAdapter(sources),
    )
    if source_config.get("cp_semantics") in {"replicated", "ownership"}:
        assert {op.source_placement.rank.cp for op in writer.plan.operations} == {0}
    for operation in writer.plan.operations:
        writer.write_tensor("tensor", operation.source_placement.fragment_id)
    manifest = writer.commit()
    # Persistence stores one logical copy, regardless of DP/PP/CP replication.
    assert sum(fragment.nbytes for fragment in manifest.fragments) == 4 * 6 * 8
    loaded = weight_store.load_manifest(manifest.manifest_key)
    assert loaded == manifest
    load_plan = weight_store.plan_load(loaded, targets.placement, targets.bindings)
    load_all(weight_store, load_plan, targets)
    for fragment in bound_fragments(targets):
        assert bytes(fragment.owner) == expected_multi_dim_fragment(fragment.placement)
    assert store.range_get_calls > 0
    assert max(store.range_sizes) <= 7
    assert max(store.range_batch_sizes) <= 3
    assert not store.registered


@pytest.mark.parametrize("owner_order", [(0, 1), (1, 0), (1, 2), (2, 1)])
def test_cp_owner_selection_is_complete_and_order_independent(owner_order):
    source = cp_inputs("source", cp=3, cp_semantics="ownership")
    binding_by_participant = {
        binding.participant_id: binding for binding in source.bindings
    }
    participants = []
    for cp_rank in owner_order:
        for part in source.placement.parts:
            if part.rank.cp != cp_rank:
                continue
            binding = binding_by_participant[part.participant_id]
            participants.append(
                RuntimeParticipant(
                    participant_id=part.participant_id,
                    rank=part.rank,
                    instance_id=binding.instance_id,
                    placement_fragments=part.fragments,
                    binding_fragments=binding.fragments,
                )
            )
    selected_source = make_runtime_inputs(
        resource_id=source.placement.resource_id,
        revision=source.placement.revision,
        placement_set_id="selected-cp-owners",
        tensors=source.placement.tensors,
        participants=tuple(participants),
    )
    targets = cp_inputs("target", cp=3, cp_semantics="ownership")
    runtime_plan = plan_transfer(selected_source, targets)
    assert {op.source.rank.cp for op in runtime_plan.operations} == {min(owner_order)}
    assert {op.target.rank.cp for op in runtime_plan.operations} == {0, 1, 2}
    _, weight_store = make_weight_store()
    upload_plan = weight_store.plan_upload(
        selected_source.placement, selected_source.bindings
    )
    assert {op.source_placement.rank.cp for op in upload_plan.operations} == {
        min(owner_order)
    }


def test_cp_partial_owner_cannot_borrow_coverage_from_another_owner():
    placement = cp_inputs("source", cp_semantics="ownership").placement
    fragments = tuple(
        fragment
        for fragment in placement.fragments
        if not (fragment.rank.cp == 1 and fragment.rank.tp == 1)
    )
    with pytest.raises(ValueError, match="not fully covered"):
        WeightPlacementManifest.from_fragments(
            resource_id=placement.resource_id,
            revision=placement.revision,
            weight_generation=placement.weight_generation,
            placement_set_id=placement.placement_set_id,
            topology=placement.topology,
            tensors=placement.tensors,
            fragments=fragments,
        )


def test_cp_json_round_trip_and_identity():
    placement = cp_inputs("source").placement
    encoded = weight_placement_to_json(placement)
    decoded = weight_placement_from_json(encoded)
    assert decoded == placement
    assert weight_placement_to_json(decoded) == encoded
    assert decoded.topology.cp_size == 2
    assert {fragment.rank.cp for fragment in decoded.fragments} == {0, 1}
    # Same logical region on two CP replicas must have distinct fragment IDs.
    fragment = placement.fragments[0]
    other_rank = replace(fragment.rank, cp=1 - fragment.rank.cp)
    other = PlacementFragment(
        tensor_id=fragment.tensor_id,
        global_offset=fragment.global_offset,
        local_shape=fragment.local_shape,
        nbytes=fragment.nbytes,
        rank=other_rank,
    )
    assert other.fragment_id != fragment.fragment_id
    changed = replace(placement.topology, cp_size=3, topology_id=None)
    assert changed.topology_id != placement.topology.topology_id


def test_pre_cp_wire_and_identity_are_unchanged():
    # Captured from main@2ca843e0, before adding the CP fields.
    encoded = (
        (Path(__file__).parent / "fixtures" / "pre_cp_placement.json")
        .read_text()
        .strip()
    )
    placement = weight_placement_from_json(encoded)
    assert placement.topology.cp_size == 1
    assert all(fragment.rank.cp == 0 for fragment in placement.fragments)
    assert (
        placement.digest
        == "a2ade0db5b17691e6a7c16d589d0498a64359eef91589addc870590899512d98"
    )
    assert weight_placement_to_json(placement) == encoded
    fragment = PlacementFragment(
        tensor_id="tensor",
        global_offset=(0, 0),
        local_shape=(2, 4),
        nbytes=8,
        rank=ParallelRank(),
    )
    assert (
        fragment.fragment_id
        == "sha256:a2622eb35ef9901ef29fa9d6a4ecd4f0210c4bca6501ca8235f029f9ba8cff79"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("cp_size", 0),
        ("cp_size", True),
        ("cp_size", "2"),
        ("cp", -1),
        ("cp", True),
        ("cp", "1"),
        ("cp", 2),
        ("context_parallel_size", 2),
    ],
)
def test_cp_wire_rejects_invalid_or_unknown_fields(field, value):
    payload = json.loads(weight_placement_to_json(cp_inputs("source").placement))
    if field == "cp":
        payload["topology"]["participants"][0]["rank"][field] = value
    else:
        payload["topology"][field] = value
    with pytest.raises(ValueError):
        weight_placement_from_json(json.dumps(payload))


def test_cp_rank_changes_cannot_reuse_placement_identity():
    payload = json.loads(weight_placement_to_json(cp_inputs("source").placement))
    for participant in payload["topology"]["participants"]:
        participant["rank"]["cp"] = 1 - participant["rank"].get("cp", 0)
    with pytest.raises(ValueError, match="topology_id"):
        weight_placement_from_json(json.dumps(payload))


@pytest.mark.parametrize("mode", ["undeclared", "missing_shard", "missing_replica"])
def test_cp_requires_explicit_semantics_and_complete_coverage(mode):
    placement = cp_inputs(
        "source",
        cp_semantics="replicated" if mode == "missing_replica" else "split",
    ).placement
    tensors, fragments = placement.tensors, placement.fragments
    if mode == "undeclared":
        # Preserve complete tensor geometry while dropping CP replica semantics.
        placement = cp_inputs("source", cp_semantics="replicated").placement
        tensors = (
            replace(
                placement.tensors[0],
                parallel_axes=tuple(
                    axis
                    for axis in placement.tensors[0].parallel_axes
                    if axis.kind != "cp"
                ),
            ),
        )
        fragments = placement.fragments
    else:
        fragments = tuple(fragment for fragment in fragments if fragment.rank.cp == 0)
    with pytest.raises(
        ValueError, match="explicit parallel semantics|not fully covered|participant"
    ):
        WeightPlacementManifest.from_fragments(
            resource_id=placement.resource_id,
            revision=placement.revision,
            weight_generation=placement.weight_generation,
            placement_set_id=placement.placement_set_id,
            topology=placement.topology,
            tensors=tensors,
            fragments=fragments,
        )


@pytest.mark.parametrize("cp", [-1, True, "1"])
def test_cp_rank_validation(cp):
    with pytest.raises(ValueError):
        ParallelRank(cp=cp)


def test_cp_topology_bounds_and_duplicate_coordinates():
    rank = ParallelRank(cp=1)
    participant = TopologyParticipant("worker", rank)
    with pytest.raises(ValueError, match="cp rank is out of range"):
        ParallelTopology(
            tp_size=1, pp_size=1, ep_size=1, dp_size=1, participants=(participant,)
        )
    with pytest.raises(ValueError, match="duplicate topology parallel rank"):
        ParallelTopology(
            tp_size=1,
            pp_size=1,
            ep_size=1,
            dp_size=1,
            cp_size=2,
            participants=(participant, TopologyParticipant("other", rank)),
        )
