"""Contract tests for the canonical framework/core boundary."""

import json
from dataclasses import fields
from typing import Any, Protocol, get_type_hints

import pytest

from mooncake.reshard.contracts import (
    PlacementManifest,
    ResourceManifest,
    RuntimeBindingManifest,
)
from mooncake.reshard.weight import (
    OwnershipAxis,
    ParallelRank,
    ParallelTopology,
    ReplicatedAxis,
    RuntimeBindingFragment,
    SplitAxis,
    TensorDescriptor,
    TopologyParticipant,
    WeightPlacementPart,
    WeightRuntimeBindingManifest,
    weight_placement_from_json,
    weight_placement_to_json,
)

from .helpers import descriptor, placement_fragment, placement_manifest


def test_resource_contracts_are_structural_protocols() -> None:
    assert issubclass(ResourceManifest, Protocol)
    assert issubclass(PlacementManifest, Protocol)
    assert issubclass(RuntimeBindingManifest, Protocol)


def test_core_exposes_only_typed_manifest_constructors() -> None:
    assert not hasattr(WeightPlacementPart, "from_runtime_inventory")
    assert not hasattr(WeightRuntimeBindingManifest, "from_runtime_inventory")
    assert get_type_hints(RuntimeBindingFragment)["owner"] != Any


def test_tensor_descriptor_has_one_canonical_shard_representation() -> None:
    assert "partition_dim" not in {field.name for field in fields(TensorDescriptor)}

    descriptor = TensorDescriptor(
        tensor_id="decoder.layer.0.mlp.weight",
        global_shape=(8, 16),
        dtype="float16",
        itemsize=2,
        shard_dims=(0,),
        layout_fingerprint="row-major",
        parallel_axes=(SplitAxis(kind="tp", dim=0),),
    )

    assert descriptor.shard_dims == (0,)


def test_parallel_axis_semantics_are_explicit() -> None:
    assert SplitAxis(kind="tp", dim=1).dim == 1
    assert ReplicatedAxis(kind="dp").kind == "dp"
    assert OwnershipAxis(kind="pp").kind == "pp"

    with pytest.raises(ValueError, match="EP must split"):
        SplitAxis(kind="ep", dim=1)


def test_weight_placement_serde_accepts_only_canonical_wire_fields() -> None:
    placement = placement_manifest()
    encoded = weight_placement_to_json(placement)

    assert weight_placement_from_json(encoded) == placement
    assert "partition_dim" not in encoded


@pytest.mark.parametrize("alias", ["model_id", "partition_dim", "split_dim"])
def test_weight_placement_serde_rejects_framework_and_legacy_aliases(
    alias: str,
) -> None:
    payload = json.loads(weight_placement_to_json(placement_manifest()))
    if alias == "model_id":
        payload[alias] = payload.pop("resource_id")
    elif alias == "partition_dim":
        payload["tensors"][0][alias] = payload["tensors"][0]["shard_dims"][0]
    else:
        axis = payload["tensors"][0]["parallel_axes"][0]
        axis[alias] = axis.pop("dim")

    with pytest.raises(ValueError, match="schema fields"):
        weight_placement_from_json(json.dumps(payload))


def test_split_axis_collectively_covers_once_with_all_split_ranks() -> None:
    topology = _two_rank_topology("tp")
    tensor = descriptor(
        global_shape=(8, 4),
        shard_dims=(0,),
        expert_id=None,
        parallel_axes=(SplitAxis(kind="tp", dim=0),),
    )

    placement = placement_manifest(
        topology=topology,
        tensors=(tensor,),
        fragments=(
            placement_fragment(
                placement_fragment_id="tp-0",
                local_shape=(4, 4),
                nbytes=32,
                rank=ParallelRank(tp=0),
            ),
            placement_fragment(
                placement_fragment_id="tp-1",
                global_offset=(4, 0),
                local_shape=(4, 4),
                nbytes=32,
                rank=ParallelRank(tp=1),
            ),
        ),
    )

    assert len(placement.fragments) == 2


def test_replicated_axis_requires_an_independent_cover_for_every_rank() -> None:
    topology = _two_rank_topology("tp")
    tensor = descriptor(
        shard_dims=(),
        expert_id=None,
        parallel_axes=(ReplicatedAxis(kind="tp"),),
    )

    with pytest.raises(ValueError, match="replicated-axis participant"):
        placement_manifest(
            topology=topology,
            tensors=(tensor,),
            fragments=(placement_fragment(rank=ParallelRank(tp=0)),),
        )


def test_replicated_axis_accepts_independent_complete_covers() -> None:
    topology = _two_rank_topology("tp")
    tensor = descriptor(
        shard_dims=(),
        expert_id=None,
        parallel_axes=(ReplicatedAxis(kind="tp"),),
    )

    placement = placement_manifest(
        topology=topology,
        tensors=(tensor,),
        fragments=(
            placement_fragment(
                placement_fragment_id="replica-0",
                rank=ParallelRank(tp=0),
            ),
            placement_fragment(
                placement_fragment_id="replica-1",
                rank=ParallelRank(tp=1),
            ),
        ),
    )

    assert len(placement.fragments) == 2


def test_ownership_axis_requires_only_declared_owners() -> None:
    topology = _two_rank_topology("pp")
    tensor = descriptor(
        shard_dims=(),
        expert_id=None,
        parallel_axes=(OwnershipAxis(kind="pp"),),
    )

    placement = placement_manifest(
        topology=topology,
        tensors=(tensor,),
        fragments=(placement_fragment(rank=ParallelRank(pp=0)),),
    )

    assert {fragment.rank.pp for fragment in placement.fragments} == {0}


def test_ownership_axis_rejects_cover_split_across_declared_owners() -> None:
    topology = _two_rank_topology("pp")
    tensor = descriptor(
        global_shape=(8, 4),
        shard_dims=(0,),
        expert_id=None,
        parallel_axes=(
            OwnershipAxis(kind="pp"),
            SplitAxis(kind="tp", dim=0),
        ),
    )

    with pytest.raises(ValueError, match="not fully covered"):
        placement_manifest(
            topology=topology,
            tensors=(tensor,),
            fragments=(
                placement_fragment(
                    placement_fragment_id="owner-0",
                    local_shape=(4, 4),
                    nbytes=32,
                    rank=ParallelRank(pp=0),
                ),
                placement_fragment(
                    placement_fragment_id="owner-1",
                    global_offset=(4, 0),
                    local_shape=(4, 4),
                    nbytes=32,
                    rank=ParallelRank(pp=1),
                ),
            ),
        )


def _two_rank_topology(kind: str) -> ParallelTopology:
    first = ParallelRank()
    second_values = {"dp": 0, "tp": 0, "pp": 0, "ep": 0}
    second_values[kind] = 1
    second = ParallelRank(**second_values)
    return ParallelTopology(
        tp_size=2 if kind == "tp" else 1,
        pp_size=2 if kind == "pp" else 1,
        ep_size=2 if kind == "ep" else 1,
        dp_size=2 if kind == "dp" else 1,
        participants=(
            TopologyParticipant("worker-0", first),
            TopologyParticipant("worker-1", second),
        ),
    )
