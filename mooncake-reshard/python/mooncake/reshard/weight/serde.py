"""Canonical JSON serialization for model-weight placement manifests.

This is the only untyped wire boundary in the manifest package. It accepts JSON
values, rejects unknown or malformed fields, and returns only canonical typed
domain values. Framework runtime objects are deliberately not accepted here.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import Optional, Union, cast

from .._typing import TypeAlias

from ..contracts import (
    ParticipantId,
    PlacementFragmentId,
    PlacementId,
    PlacementSetId,
    ResourceId,
    ResourceKind,
    RevisionId,
    TensorId,
    TopologyId,
)
from .part import WeightPlacementPart
from .placement import WeightPlacementManifest
from .topology import ParallelTopology, TopologyParticipant
from .types import (
    OwnershipAxis,
    ParallelAxis,
    ParallelAxisKind,
    ParallelRank,
    PlacementFragment,
    ReplicatedAxis,
    SplitAxis,
    SplitAxisKind,
    TensorDescriptor,
)

JsonScalar: TypeAlias = Union[None, bool, int, float, str]
JsonValue: TypeAlias = Union[JsonScalar, list["JsonValue"], dict[str, "JsonValue"]]
JsonObject: TypeAlias = Mapping[str, JsonValue]


def weight_placement_to_json(placement: WeightPlacementManifest) -> str:
    """Serialize one typed placement using the canonical wire schema."""

    if not isinstance(placement, WeightPlacementManifest):
        raise ValueError("placement must be a WeightPlacementManifest")  # noqa: TRY004
    payload: dict[str, JsonValue] = {
        "resource_kind": placement.resource_kind.value,
        "resource_id": placement.resource_id,
        "revision": placement.revision,
        "weight_generation": placement.weight_generation,
        "placement_set_id": placement.placement_set_id,
        "placement_id": placement.placement_id,
        "topology": _topology_to_wire(placement.topology),
        "tensors": [_tensor_to_wire(tensor) for tensor in placement.tensors],
        "parts": [_part_to_wire(part) for part in placement.parts],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def weight_placement_from_json(value: str) -> WeightPlacementManifest:
    """Parse only the canonical model-weight placement wire schema."""

    manifest = _require_exact_fields(
        _load_json_object(value, "placement manifest"),
        {
            "resource_kind",
            "resource_id",
            "revision",
            "weight_generation",
            "placement_set_id",
            "placement_id",
            "topology",
            "tensors",
            "parts",
        },
        "placement manifest",
    )
    if _require_nonempty_string(manifest["resource_kind"], "resource_kind") != (
        ResourceKind.MODEL_WEIGHT.value
    ):
        raise ValueError("placement manifest resource_kind must be model_weight")

    resource_id = ResourceId(
        _require_nonempty_string(manifest["resource_id"], "resource_id")
    )
    revision = RevisionId(_require_nonempty_string(manifest["revision"], "revision"))
    weight_generation = _require_integer(
        manifest["weight_generation"], "weight_generation", minimum=0
    )
    placement_set_id = PlacementSetId(
        _require_nonempty_string(manifest["placement_set_id"], "placement_set_id")
    )
    placement_id = PlacementId(
        _require_nonempty_string(manifest["placement_id"], "placement_id")
    )
    topology = _topology_from_wire(manifest["topology"])
    tensors = tuple(
        _tensor_from_wire(item, index)
        for index, item in enumerate(
            _require_sequence(manifest["tensors"], "placement tensors")
        )
    )
    tensor_by_id = {tensor.tensor_id: tensor for tensor in tensors}
    if len(tensor_by_id) != len(tensors):
        raise ValueError("duplicate tensor_id in placement JSON")

    parts: list[WeightPlacementPart] = []
    referenced_tensor_ids: set[TensorId] = set()
    for index, item in enumerate(
        _require_sequence(manifest["parts"], "placement parts")
    ):
        part = _require_exact_fields(
            item,
            {"participant_id", "rank", "fragments"},
            f"placement part {index}",
        )
        participant_id = ParticipantId(
            _require_nonempty_string(part["participant_id"], "participant_id")
        )
        rank = _rank_from_wire(part["rank"], f"placement part rank {index}")
        fragments = tuple(
            _fragment_from_wire(fragment, fragment_index)
            for fragment_index, fragment in enumerate(
                _require_sequence(part["fragments"], "placement part fragments")
            )
        )
        local_tensor_ids = {fragment.tensor_id for fragment in fragments}
        referenced_tensor_ids.update(local_tensor_ids)
        unknown = sorted(local_tensor_ids - set(tensor_by_id))
        if unknown:
            raise ValueError(f"unknown tensor_id in placement part: {unknown[0]}")
        parts.append(
            WeightPlacementPart(
                resource_id=resource_id,
                revision=revision,
                weight_generation=weight_generation,
                placement_set_id=placement_set_id,
                topology_id=topology.topology_id,
                participant_id=participant_id,
                rank=rank,
                tensors=tuple(
                    tensor_by_id[tensor_id] for tensor_id in sorted(local_tensor_ids)
                ),
                fragments=fragments,
            )
        )

    unreferenced_tensor_ids = sorted(set(tensor_by_id) - referenced_tensor_ids)
    if unreferenced_tensor_ids:
        raise ValueError(
            "placement JSON contains an unreferenced tensor: "
            f"{unreferenced_tensor_ids[0]}"
        )
    return WeightPlacementManifest(
        resource_id=resource_id,
        revision=revision,
        weight_generation=weight_generation,
        placement_set_id=placement_set_id,
        placement_id=placement_id,
        topology=topology,
        parts=tuple(parts),
    )


def _part_to_wire(part: WeightPlacementPart) -> dict[str, JsonValue]:
    return {
        "participant_id": part.participant_id,
        "rank": _rank_to_wire(part.rank),
        "fragments": [_fragment_to_wire(fragment) for fragment in part.fragments],
    }


def _topology_to_wire(topology: ParallelTopology) -> dict[str, JsonValue]:
    return {
        "tp_size": topology.tp_size,
        "pp_size": topology.pp_size,
        "ep_size": topology.ep_size,
        "dp_size": topology.dp_size,
        "topology_id": topology.topology_id,
        "participants": [
            {
                "participant_id": participant.participant_id,
                "rank": _rank_to_wire(participant.rank),
            }
            for participant in topology.participants
        ],
    }


def _topology_from_wire(value: object) -> ParallelTopology:
    topology = _require_exact_fields(
        value,
        {"tp_size", "pp_size", "ep_size", "dp_size", "topology_id", "participants"},
        "parallel topology",
    )
    participants: list[TopologyParticipant] = []
    for index, item in enumerate(
        _require_sequence(topology["participants"], "parallel topology participants")
    ):
        participant = _require_exact_fields(
            item,
            {"participant_id", "rank"},
            f"parallel topology participant {index}",
        )
        participants.append(
            TopologyParticipant(
                participant_id=ParticipantId(
                    _require_nonempty_string(
                        participant["participant_id"], "participant_id"
                    )
                ),
                rank=_rank_from_wire(
                    participant["rank"], f"parallel topology rank {index}"
                ),
            )
        )
    return ParallelTopology(
        tp_size=_require_integer(topology["tp_size"], "tp_size", minimum=1),
        pp_size=_require_integer(topology["pp_size"], "pp_size", minimum=1),
        ep_size=_require_integer(topology["ep_size"], "ep_size", minimum=1),
        dp_size=_require_integer(topology["dp_size"], "dp_size", minimum=1),
        participants=tuple(participants),
        topology_id=TopologyId(
            _require_nonempty_string(topology["topology_id"], "topology_id")
        ),
    )


def _tensor_to_wire(tensor: TensorDescriptor) -> dict[str, JsonValue]:
    return {
        "tensor_id": tensor.tensor_id,
        "global_shape": list(tensor.global_shape),
        "dtype": tensor.dtype,
        "itemsize": tensor.itemsize,
        "shard_dims": list(tensor.shard_dims),
        "layout_fingerprint": tensor.layout_fingerprint,
        "parallel_axes": [_axis_to_wire(axis) for axis in tensor.parallel_axes],
        "layer_id": tensor.layer_id,
        "expert_id": tensor.expert_id,
    }


def _tensor_from_wire(value: object, index: int) -> TensorDescriptor:
    tensor = _require_exact_fields(
        value,
        {
            "tensor_id",
            "global_shape",
            "dtype",
            "itemsize",
            "shard_dims",
            "layout_fingerprint",
            "parallel_axes",
            "layer_id",
            "expert_id",
        },
        f"placement tensor {index}",
    )
    return TensorDescriptor(
        tensor_id=TensorId(_require_nonempty_string(tensor["tensor_id"], "tensor_id")),
        global_shape=_integer_tuple(tensor["global_shape"], "global_shape", minimum=1),
        dtype=_require_nonempty_string(tensor["dtype"], "dtype"),
        itemsize=_require_integer(tensor["itemsize"], "itemsize", minimum=1),
        shard_dims=_integer_tuple(tensor["shard_dims"], "shard_dims", minimum=0),
        layout_fingerprint=_require_nonempty_string(
            tensor["layout_fingerprint"], "layout_fingerprint"
        ),
        parallel_axes=tuple(
            _axis_from_wire(axis, axis_index)
            for axis_index, axis in enumerate(
                _require_sequence(
                    tensor["parallel_axes"], "placement tensor parallel_axes"
                )
            )
        ),
        layer_id=_optional_integer(tensor["layer_id"], "layer_id", minimum=0),
        expert_id=_optional_integer(tensor["expert_id"], "expert_id", minimum=0),
    )


def _axis_to_wire(axis: ParallelAxis) -> dict[str, JsonValue]:
    if isinstance(axis, SplitAxis):
        return {"semantics": "split", "kind": axis.kind, "dim": axis.dim}
    if isinstance(axis, ReplicatedAxis):
        return {"semantics": "replicated", "kind": axis.kind}
    if isinstance(axis, OwnershipAxis):
        return {"semantics": "ownership", "kind": axis.kind}
    raise ValueError("unsupported parallel axis value")


def _axis_from_wire(value: object, index: int) -> ParallelAxis:
    axis = _require_mapping(value, f"placement tensor parallel axis {index}")
    semantics = _require_nonempty_string(axis.get("semantics"), "axis semantics")
    if semantics == "split":
        split = _require_exact_fields(
            axis,
            {"semantics", "kind", "dim"},
            f"placement tensor parallel axis {index}",
        )
        return SplitAxis(
            kind=_split_axis_kind(split["kind"]),
            dim=_require_integer(split["dim"], "split axis dim", minimum=0),
        )
    if semantics == "replicated":
        replicated = _require_exact_fields(
            axis,
            {"semantics", "kind"},
            f"placement tensor parallel axis {index}",
        )
        return ReplicatedAxis(kind=_parallel_axis_kind(replicated["kind"]))
    if semantics == "ownership":
        ownership = _require_exact_fields(
            axis,
            {"semantics", "kind"},
            f"placement tensor parallel axis {index}",
        )
        return OwnershipAxis(kind=_parallel_axis_kind(ownership["kind"]))
    raise ValueError(f"unsupported parallel axis semantics: {semantics}")


def _fragment_to_wire(fragment: PlacementFragment) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {
        "placement_fragment_id": fragment.placement_fragment_id,
        "tensor_id": fragment.tensor_id,
        "global_offset": list(fragment.global_offset),
        "local_shape": list(fragment.local_shape),
        "nbytes": fragment.nbytes,
        "rank": _rank_to_wire(fragment.rank),
        "aliases": list(fragment.aliases),
    }
    if fragment.pipeline_stage_id is not None:
        result["pipeline_stage_id"] = fragment.pipeline_stage_id
    return result


def _fragment_from_wire(value: object, index: int) -> PlacementFragment:
    fragment = _require_mapping(value, f"placement fragment {index}")
    expected_fields = {
        "placement_fragment_id",
        "tensor_id",
        "global_offset",
        "local_shape",
        "nbytes",
        "rank",
        "pipeline_stage_id",
        "aliases",
    }
    legacy_fields = expected_fields - {"pipeline_stage_id"}
    fragment_fields = set(fragment)
    if fragment_fields != expected_fields and fragment_fields != legacy_fields:
        raise ValueError(f"placement fragment {index} has an invalid schema")
    return PlacementFragment(
        placement_fragment_id=PlacementFragmentId(
            _require_nonempty_string(
                fragment["placement_fragment_id"], "placement_fragment_id"
            )
        ),
        tensor_id=TensorId(
            _require_nonempty_string(fragment["tensor_id"], "tensor_id")
        ),
        global_offset=_integer_tuple(
            fragment["global_offset"], "global_offset", minimum=0
        ),
        local_shape=_integer_tuple(fragment["local_shape"], "local_shape", minimum=1),
        nbytes=_require_integer(fragment["nbytes"], "nbytes", minimum=1),
        rank=_rank_from_wire(fragment["rank"], f"placement rank {index}"),
        pipeline_stage_id=_optional_integer(
            fragment.get("pipeline_stage_id"),
            "pipeline_stage_id",
            minimum=0,
        ),
        aliases=tuple(
            TensorId(_require_nonempty_string(alias, "alias"))
            for alias in _require_sequence(fragment["aliases"], "aliases")
        ),
    )


def _rank_to_wire(rank: ParallelRank) -> dict[str, JsonValue]:
    return {"dp": rank.dp, "tp": rank.tp, "pp": rank.pp, "ep": rank.ep}


def _rank_from_wire(value: object, label: str) -> ParallelRank:
    rank = _require_exact_fields(value, {"dp", "tp", "pp", "ep"}, label)
    return ParallelRank(
        dp=_require_integer(rank["dp"], "rank dp", minimum=0),
        tp=_require_integer(rank["tp"], "rank tp", minimum=0),
        pp=_require_integer(rank["pp"], "rank pp", minimum=0),
        ep=_require_integer(rank["ep"], "rank ep", minimum=0),
    )


def _load_json_object(value: str, label: str) -> JsonObject:
    def reject_constant(constant: str) -> None:
        raise ValueError(f"non-finite JSON number is unsupported: {constant}")

    def reject_duplicate_fields(
        pairs: list[tuple[str, object]],
    ) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON field: {key}")
            result[key] = cast(JsonValue, item)
        return result

    try:
        raw = json.loads(
            value,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate_fields,
        )
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is not valid JSON") from error
    return _require_mapping(cast(object, raw), label)


def _require_mapping(value: object, label: str) -> JsonObject:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a JSON object")  # noqa: TRY004
    mapping = cast(Mapping[object, object], value)
    if any(type(key) is not str for key in mapping):
        raise ValueError(f"{label} must be a JSON object")
    return cast(JsonObject, mapping)


def _require_exact_fields(
    value: object,
    expected: AbstractSet[str],
    label: str,
) -> JsonObject:
    mapping = _require_mapping(value, label)
    if set(mapping) != set(expected):
        raise ValueError(f"{label} schema fields do not match contract")
    return mapping


def _require_sequence(value: object, label: str) -> Sequence[JsonValue]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError(f"{label} must be a sequence")  # noqa: TRY004
    return cast(Sequence[JsonValue], value)


def _require_nonempty_string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _require_integer(value: object, label: str, *, minimum: int) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer at least {minimum}")
    return value


def _optional_integer(
    value: object,
    label: str,
    *,
    minimum: int,
) -> Optional[int]:
    if value is None:
        return None
    return _require_integer(value, label, minimum=minimum)


def _integer_tuple(
    value: object,
    label: str,
    *,
    minimum: int,
) -> tuple[int, ...]:
    return tuple(
        _require_integer(item, label, minimum=minimum)
        for item in _require_sequence(value, label)
    )


def _parallel_axis_kind(value: object) -> ParallelAxisKind:
    kind = _require_nonempty_string(value, "parallel axis kind")
    if kind not in {"dp", "pp", "ep", "tp"}:
        raise ValueError(f"unsupported parallel axis kind: {kind}")
    return cast(ParallelAxisKind, kind)


def _split_axis_kind(value: object) -> SplitAxisKind:
    kind = _parallel_axis_kind(value)
    if kind not in {"ep", "tp"}:
        raise ValueError(f"{kind} cannot use split semantics")
    return cast(SplitAxisKind, kind)
