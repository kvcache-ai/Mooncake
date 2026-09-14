from __future__ import annotations

import json
import pytest

from mooncake.reshard.weight import (
    OwnershipAxis,
    ParallelRank,
    ReplicatedAxis,
    SplitAxis,
    weight_placement_from_json,
    weight_placement_to_json,
)

from .helpers import (
    descriptor,
    parallel_topology,
    placement_fragment,
    placement_manifest,
)


def test_placement_round_trip_is_stable_and_address_free() -> None:
    placement = placement_manifest()

    encoded = weight_placement_to_json(placement)
    decoded = weight_placement_from_json(encoded)

    assert decoded == placement
    assert decoded.digest == placement.digest
    assert placement.placement_id == (
        "sha256:618f22d9994d8d327a7afd97f3cfcf1f6680a337d71ea3a8162a7827ed0346db"
    )
    assert placement.digest == (
        "a2ade0db5b17691e6a7c16d589d0498a64359eef91589addc870590899512d98"
    )
    assert encoded == weight_placement_to_json(placement)
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


def test_explicit_pipeline_stage_is_canonical_and_round_trips() -> None:
    legacy = placement_manifest()
    staged = placement_manifest(
        fragments=(placement_fragment(pipeline_stage_id=3),),
    )

    encoded = weight_placement_to_json(staged)

    assert '"pipeline_stage_id":3' in encoded
    assert weight_placement_from_json(encoded) == staged
    assert staged.placement_id != legacy.placement_id


def test_placement_digest_is_independent_of_inventory_order() -> None:
    tensors = (
        descriptor(
            tensor_id="b.weight",
            shard_dims=(),
            parallel_axes=(OwnershipAxis(kind="tp"),),
            expert_id=None,
        ),
        descriptor(
            tensor_id="a.weight",
            shard_dims=(),
            parallel_axes=(OwnershipAxis(kind="tp"),),
            expert_id=None,
        ),
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
    assert weight_placement_to_json(first) == weight_placement_to_json(second)
    assert first.digest == second.digest


def test_weight_generation_changes_canonical_placement_identity() -> None:
    generation_1 = placement_manifest(weight_generation=1)
    generation_2 = placement_manifest(weight_generation=2)

    assert generation_1.revision == generation_2.revision
    assert generation_1.placement_id != generation_2.placement_id
    assert generation_1.digest != generation_2.digest
    assert (
        weight_placement_from_json(
            weight_placement_to_json(generation_2)
        ).weight_generation
        == 2
    )


def test_placement_uses_one_canonical_shard_representation() -> None:
    placement = placement_manifest(tensors=(descriptor(shard_dims=(0,)),))

    assert placement.tensors[0].shard_dims == (0,)
    assert "partition_dim" not in weight_placement_to_json(placement)


@pytest.mark.parametrize("mutation", ["missing", "unknown", "nan"])
def test_placement_json_requires_strict_schema(mutation: str) -> None:
    raw = json.loads(weight_placement_to_json(placement_manifest()))
    if mutation == "missing":
        del raw["revision"]
    elif mutation == "unknown":
        raw["future_semantics"] = "required"
    else:
        raw["resource_id"] = float("nan")

    with pytest.raises(ValueError):
        weight_placement_from_json(json.dumps(raw))


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
    raw = json.loads(weight_placement_to_json(placement_manifest()))
    target = raw
    for component in path:
        target = target[component]
    operation, field = mutation
    if operation == "pop":
        target.pop(field)
    else:
        target[field] = "unsupported"

    with pytest.raises(ValueError, match="schema"):
        weight_placement_from_json(json.dumps(raw))


@pytest.mark.parametrize("value", ["not-json", "[]", '"placement"'])
def test_placement_json_rejects_invalid_document(value: str) -> None:
    with pytest.raises(ValueError):
        weight_placement_from_json(value)


def test_placement_json_rejects_duplicate_object_keys() -> None:
    encoded = weight_placement_to_json(placement_manifest())
    duplicated = encoded.replace(
        '"resource_id":"model"',
        '"resource_id":"model","resource_id":"other"',
        1,
    )

    with pytest.raises(ValueError, match="duplicate JSON field"):
        weight_placement_from_json(duplicated)


def test_placement_json_rejects_unreferenced_tensor_descriptors() -> None:
    raw = json.loads(weight_placement_to_json(placement_manifest()))
    orphan = dict(raw["tensors"][0])
    orphan["tensor_id"] = "orphan.weight"
    raw["tensors"].append(orphan)

    with pytest.raises(ValueError, match="unreferenced tensor"):
        weight_placement_from_json(json.dumps(raw))


@pytest.mark.parametrize("aliases", ["alias", {"alias": 1}, ["alias", "alias"]])
def test_placement_json_rejects_invalid_aliases(aliases) -> None:
    raw = json.loads(weight_placement_to_json(placement_manifest()))
    raw["parts"][0]["fragments"][0]["aliases"] = aliases

    with pytest.raises(ValueError, match="aliases"):
        weight_placement_from_json(json.dumps(raw))


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
    raw = json.loads(weight_placement_to_json(placement_manifest()))
    target = raw
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value

    with pytest.raises(ValueError):
        weight_placement_from_json(json.dumps(raw))


def test_placement_id_must_match_canonical_logical_content() -> None:
    with pytest.raises(ValueError, match="canonical logical content"):
        placement_manifest(placement_id="opaque-placement-id")


def test_parallel_axis_semantics_participate_in_placement_identity() -> None:
    tp = placement_manifest(
        tensors=(
            descriptor(
                expert_id=None,
                parallel_axes=(SplitAxis(kind="tp", dim=0),),
            ),
        ),
    )
    ep = placement_manifest(
        tensors=(
            descriptor(
                expert_id=None,
                parallel_axes=(SplitAxis(kind="ep", dim=0),),
            ),
        ),
    )

    assert tp.placement_id != ep.placement_id
    assert tp.digest != ep.digest


def test_declared_parallel_axis_sizes_participate_in_placement_identity() -> None:
    tensor = descriptor(
        shard_dims=(),
        parallel_axes=(ReplicatedAxis(kind="tp"),),
        expert_id=None,
    )
    base = placement_manifest(tensors=(tensor,))
    expanded_topology = parallel_topology(tp_size=2)
    expanded = placement_manifest(topology=expanded_topology, tensors=(tensor,))

    assert base.topology.participants == expanded.topology.participants
    assert base.placement_id != expanded.placement_id
    assert base.digest != expanded.digest


def test_axis_semantic_kind_participates_in_placement_identity() -> None:
    replicated = placement_manifest(
        tensors=(
            descriptor(
                shard_dims=(),
                expert_id=None,
                parallel_axes=(ReplicatedAxis(kind="tp"),),
            ),
        ),
    )
    ownership = placement_manifest(
        tensors=(
            descriptor(
                shard_dims=(),
                expert_id=None,
                parallel_axes=(OwnershipAxis(kind="tp"),),
            ),
        ),
    )

    assert replicated.placement_id != ownership.placement_id
    assert replicated.digest != ownership.digest
