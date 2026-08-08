from __future__ import annotations

import json
import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    TensorParallelAxis,
    TopologyParticipant,
    WeightPlacementManifest,
)

from .helpers import (
    descriptor,
    parallel_topology,
    placement_fragment,
    placement_manifest,
    placement_part_inventory,
    runtime_inventory_tensor,
)


def test_placement_round_trip_is_stable_and_address_free() -> None:
    placement = placement_manifest()

    encoded = placement.to_json()
    decoded = WeightPlacementManifest.from_json(encoded)

    assert decoded == placement
    assert decoded.digest == placement.digest
    assert placement.placement_id == (
        "sha256:75c6320800da148615e9c24ca25e0fe4df83ac29492a68c602e4c44c9162f7d4"
    )
    assert placement.digest == (
        "57c3e956bbcac04339202684d8cc8c25c18464bee338354c62fe0f42865a0d48"
    )
    assert encoded == placement.to_json()
    for forbidden in (
        "address",
        "endpoint",
        "worker_id",
        "instance_id",
        '"generation":',
        "lease_id",
        "owner",
    ):
        assert forbidden not in encoded


def test_placement_digest_is_independent_of_inventory_order() -> None:
    tensors = (
        descriptor(tensor_id="b.weight"),
        descriptor(tensor_id="a.weight"),
    )
    fragments = (
        placement_fragment(
            placement_fragment_id="b",
            tensor_id="b.weight",
            rank=ParallelRank(tp=1),
        ),
        placement_fragment(
            placement_fragment_id="a",
            tensor_id="a.weight",
            rank=ParallelRank(tp=0),
        ),
    )

    first = placement_manifest(tensors=tensors, fragments=fragments)
    second = placement_manifest(
        tensors=tuple(reversed(tensors)),
        fragments=tuple(reversed(fragments)),
    )

    assert first == second
    assert first.to_json() == second.to_json()
    assert first.digest == second.digest


def test_placement_normalizes_partition_dim_and_shard_dims() -> None:
    partitioned = placement_manifest(
        tensors=(descriptor(shard_dims=None),),
    )
    multidim_placement = placement_manifest(
        tensors=(descriptor(shard_dims=(0,)),),
    )

    assert partitioned == multidim_placement
    assert partitioned.tensors[0].shard_dims == (0,)
    assert partitioned.digest == multidim_placement.digest


@pytest.mark.parametrize("mutation", ["missing", "unknown", "nan"])
def test_placement_json_requires_strict_schema(mutation: str) -> None:
    raw = json.loads(placement_manifest().to_json())
    if mutation == "missing":
        del raw["revision"]
    elif mutation == "unknown":
        raw["future_semantics"] = "required"
    else:
        raw["resource_id"] = float("nan")

    with pytest.raises(ValueError):
        WeightPlacementManifest.from_json(json.dumps(raw))


@pytest.mark.parametrize(
    ("path", "mutation"),
    [
        (("tensors", 0), ("pop", "dtype")),
        (("tensors", 0), ("set", "future_semantics")),
        (("topology",), ("pop", "tp_size")),
        (("topology",), ("set", "future_semantics")),
        (("topology", "participants", 0), ("pop", "participant_id")),
        (("topology", "participants", 0), ("set", "future_semantics")),
        (("topology", "participants", 0, "rank"), ("pop", "tp")),
        (("tensors", 0, "parallel_axes", 0), ("pop", "kind")),
        (
            ("tensors", 0, "parallel_axes", 0),
            ("set", "future_semantics"),
        ),
        (("parts", 0), ("pop", "participant_id")),
        (("parts", 0), ("set", "future_semantics")),
        (("parts", 0, "rank"), ("set", "future_semantics")),
        (("parts", 0, "fragments", 0), ("pop", "nbytes")),
        (("parts", 0, "fragments", 0), ("set", "future_semantics")),
        (("parts", 0, "fragments", 0, "rank"), ("pop", "tp")),
        (
            ("parts", 0, "fragments", 0, "rank"),
            ("set", "future_semantics"),
        ),
    ],
)
def test_placement_json_requires_strict_nested_schema(
    path: tuple, mutation: tuple[str, str]
) -> None:
    raw = json.loads(placement_manifest().to_json())
    target = raw
    for component in path:
        target = target[component]
    operation, field = mutation
    if operation == "pop":
        target.pop(field)
    else:
        target[field] = "unsupported"

    with pytest.raises(ValueError, match="schema"):
        WeightPlacementManifest.from_json(json.dumps(raw))


@pytest.mark.parametrize("value", ["not-json", "[]", '"placement"'])
def test_placement_json_rejects_invalid_document(value: str) -> None:
    with pytest.raises(ValueError):
        WeightPlacementManifest.from_json(value)


def test_placement_json_rejects_duplicate_object_keys() -> None:
    encoded = placement_manifest().to_json()
    duplicated = encoded.replace(
        '"resource_id":"model"',
        '"resource_id":"model","resource_id":"other"',
        1,
    )

    with pytest.raises(ValueError, match="duplicate JSON field"):
        WeightPlacementManifest.from_json(duplicated)


@pytest.mark.parametrize("aliases", ["alias", {"alias": 1}, ["alias", "alias"]])
def test_placement_json_rejects_invalid_aliases(aliases) -> None:
    raw = json.loads(placement_manifest().to_json())
    raw["parts"][0]["fragments"][0]["aliases"] = aliases

    with pytest.raises(ValueError, match="aliases"):
        WeightPlacementManifest.from_json(json.dumps(raw))


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("tensors",), {}),
        (("topology", "participants"), {}),
        (("parts",), 1),
        (("parts", 0, "fragments"), 1),
        (("tensors", 0, "global_shape"), 8),
        (("tensors", 0, "shard_dims"), "0"),
        (("tensors", 0, "parallel_axes"), {}),
        (("parts", 0, "fragments", 0, "global_offset"), 0),
        (("parts", 0, "fragments", 0, "local_shape"), None),
        (("parts", 0, "fragments", 0, "rank"), []),
    ],
)
def test_placement_json_rejects_wrong_container_types(
    path: tuple, value: object
) -> None:
    raw = json.loads(placement_manifest().to_json())
    target = raw
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value

    with pytest.raises(ValueError):
        WeightPlacementManifest.from_json(json.dumps(raw))


def test_placement_inventory_is_framework_neutral() -> None:
    topology = parallel_topology()
    tensor = runtime_inventory_tensor()
    inventory = placement_part_inventory(
        topology=topology,
        tensors=(
            {
                key: value
                for key, value in tensor.items()
                if key
                in {
                    "tensor_id",
                    "global_shape",
                    "global_offset",
                    "local_shape",
                    "dtype",
                    "itemsize",
                    "partition_dim",
                    "layer_id",
                    "expert_id",
                    "layout_fingerprint",
                    "parallel_axes",
                    "nbytes",
                    "rank",
                    "aliases",
                }
            },
        ),
    )

    placement = WeightPlacementManifest.from_runtime_inventories(
        (inventory,),
        topology=topology,
    )

    assert placement.fragments[0].rank == ParallelRank()
    assert placement.fragments[0].global_offset == (0, 0)
    assert placement.fragments[0].placement_fragment_id.startswith("sha256:")


def test_placement_inventory_rejects_conflicting_resource_and_model_ids() -> None:
    topology = parallel_topology()
    tensor = runtime_inventory_tensor()
    inventory = placement_part_inventory(
        topology=topology,
        resource_id="resource-a",
        model_id="resource-b",
        tensors=(tensor,),
    )

    with pytest.raises(ValueError, match="resource_id and model_id differ"):
        WeightPlacementManifest.from_runtime_inventories(
            (inventory,),
            topology=topology,
        )


def test_placement_inventory_normalizes_equivalent_single_axis_descriptors() -> None:
    fields = {
        "placement_fragment_id",
        "tensor_id",
        "global_shape",
        "global_offset",
        "local_shape",
        "dtype",
        "itemsize",
        "partition_dim",
        "layer_id",
        "expert_id",
        "layout_fingerprint",
        "parallel_axes",
        "shard_dims",
        "nbytes",
        "rank",
        "aliases",
    }
    first = runtime_inventory_tensor(
        placement_fragment_id="placement-0",
        global_shape=(8, 4),
        global_offset=(0, 0),
        rank={"dp": 0, "tp": 0, "pp": 0, "ep": 0},
    )
    second = runtime_inventory_tensor(
        placement_fragment_id="placement-1",
        global_shape=(8, 4),
        shard_dims=(0,),
        global_offset=(4, 0),
        rank={"dp": 0, "tp": 1, "pp": 0, "ep": 0},
    )
    topology = parallel_topology(
        participants=(
            TopologyParticipant("worker-0", ParallelRank(tp=0)),
            TopologyParticipant("worker-1", ParallelRank(tp=1)),
        ),
    )
    inventories = tuple(
        placement_part_inventory(
            topology=topology,
            participant_id=f"worker-{index}",
            tensors=({key: value for key, value in tensor.items() if key in fields},),
        )
        for index, tensor in enumerate((first, second))
    )

    placement = WeightPlacementManifest.from_runtime_inventories(
        inventories,
        topology=topology,
    )

    assert placement.tensors[0].partition_dim == 0
    assert placement.tensors[0].shard_dims == (0,)
    assert len(placement.fragments) == 2


def test_placement_id_must_match_canonical_logical_content() -> None:
    with pytest.raises(ValueError, match="canonical logical content"):
        placement_manifest(placement_id="opaque-placement-id")


def test_parallel_axis_semantics_participate_in_placement_identity() -> None:
    tp = placement_manifest(
        tensors=(
            descriptor(
                expert_id=None,
                parallel_axes=(TensorParallelAxis(kind="tp", split_dim=0),),
            ),
        ),
    )
    ep = placement_manifest(
        tensors=(
            descriptor(
                expert_id=None,
                parallel_axes=(TensorParallelAxis(kind="ep", split_dim=0),),
            ),
        ),
    )

    assert tp.placement_id != ep.placement_id
    assert tp.digest != ep.digest


def test_declared_parallel_axis_sizes_participate_in_placement_identity() -> None:
    base = placement_manifest()
    expanded_topology = parallel_topology(tp_size=2)
    expanded = placement_manifest(topology=expanded_topology)

    assert base.topology.participants == expanded.topology.participants
    assert base.placement_id != expanded.placement_id
    assert base.digest != expanded.digest
