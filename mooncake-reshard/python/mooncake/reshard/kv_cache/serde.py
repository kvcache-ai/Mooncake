"""Strict JSON boundaries for canonical KV-cache contracts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import TypeAlias, cast

from ..contracts import (
    LeaseId,
    ParticipantId,
    PlacementFragmentId,
    PlacementId,
    PlacementSetId,
    ResourceId,
    ResourceKind,
    RevisionId,
    RuntimeFragmentId,
    RuntimeInstanceId,
    TopologyId,
)
from .part import KVCachePlacementPart
from .placement import KVCachePlacementManifest
from .runtime import KVCacheBufferBinding, KVCacheRuntimeBindingManifest
from .topology import KVCacheTopology, KVCacheTopologyParticipant
from .types import (
    KVCacheComponent,
    KVCacheDescriptor,
    KVCacheLayout,
    KVCacheRank,
    KVCacheRuntimeBuffer,
)

JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = Mapping[str, JsonValue]


def kv_cache_part_to_json(part: KVCachePlacementPart) -> str:
    if not isinstance(part, KVCachePlacementPart):
        raise ValueError("part must be a KVCachePlacementPart")  # noqa: TRY004
    return _dump(_part_to_wire(part))


def kv_cache_part_from_json(value: str) -> KVCachePlacementPart:
    return _part_from_wire(_load_json_object(value, "KV-cache placement part"))


def kv_cache_placement_to_json(placement: KVCachePlacementManifest) -> str:
    if not isinstance(placement, KVCachePlacementManifest):
        raise ValueError(  # noqa: TRY004
            "placement must be a KVCachePlacementManifest"
        )
    return _dump(_placement_to_wire(placement))


def kv_cache_placement_from_json(value: str) -> KVCachePlacementManifest:
    payload = _require_exact_fields(
        _load_json_object(value, "KV-cache placement"),
        {
            "resource_kind",
            "resource_id",
            "revision",
            "placement_set_id",
            "placement_id",
            "placement_digest",
            "topology",
            "descriptor",
            "parts",
        },
        "KV-cache placement",
    )
    _require_resource_kind(payload)
    topology = _topology_from_wire(payload["topology"])
    descriptor = _descriptor_from_wire(payload["descriptor"])
    result = KVCachePlacementManifest(
        resource_id=ResourceId(_string(payload["resource_id"], "resource_id")),
        revision=RevisionId(_string(payload["revision"], "revision")),
        placement_set_id=PlacementSetId(
            _string(payload["placement_set_id"], "placement_set_id")
        ),
        placement_id=PlacementId(_string(payload["placement_id"], "placement_id")),
        topology=topology,
        descriptor=descriptor,
        parts=tuple(
            _part_from_wire(item)
            for item in _sequence(payload["parts"], "placement parts")
        ),
    )
    if result.digest != _string(payload["placement_digest"], "placement_digest"):
        raise ValueError("KV-cache placement digest does not match content")
    return result


def kv_cache_runtime_binding_to_json(
    binding: KVCacheRuntimeBindingManifest,
) -> str:
    if not isinstance(binding, KVCacheRuntimeBindingManifest):
        raise ValueError(  # noqa: TRY004
            "binding must be a KVCacheRuntimeBindingManifest"
        )
    return _dump(
        {
            "resource_kind": binding.resource_kind.value,
            "resource_id": binding.resource_id,
            "placement_id": binding.placement_id,
            "placement_digest": binding.placement_digest,
            "instance_id": binding.instance_id,
            "generation": binding.generation,
            "lease_id": binding.lease_id,
            "revision": binding.revision,
            "participant_id": binding.participant_id,
            "buffers": [_buffer_to_wire(item) for item in binding.buffers],
        }
    )


def kv_cache_runtime_binding_from_json(
    value: str,
) -> KVCacheRuntimeBindingManifest:
    payload = _require_exact_fields(
        _load_json_object(value, "KV-cache runtime binding"),
        {
            "resource_kind",
            "resource_id",
            "placement_id",
            "placement_digest",
            "instance_id",
            "generation",
            "lease_id",
            "revision",
            "participant_id",
            "buffers",
        },
        "KV-cache runtime binding",
    )
    _require_resource_kind(payload)
    return KVCacheRuntimeBindingManifest(
        resource_id=ResourceId(_string(payload["resource_id"], "resource_id")),
        placement_id=PlacementId(_string(payload["placement_id"], "placement_id")),
        placement_digest=_string(payload["placement_digest"], "placement_digest"),
        instance_id=RuntimeInstanceId(_string(payload["instance_id"], "instance_id")),
        generation=_integer(payload["generation"], "generation"),
        lease_id=LeaseId(_string(payload["lease_id"], "lease_id")),
        revision=RevisionId(_string(payload["revision"], "revision")),
        participant_id=ParticipantId(
            _string(payload["participant_id"], "participant_id")
        ),
        buffers=tuple(
            _buffer_from_wire(item)
            for item in _sequence(payload["buffers"], "runtime buffers")
        ),
    )


def _placement_to_wire(placement: KVCachePlacementManifest) -> dict[str, JsonValue]:
    return {
        "resource_kind": placement.resource_kind.value,
        "resource_id": placement.resource_id,
        "revision": placement.revision,
        "placement_set_id": placement.placement_set_id,
        "placement_id": placement.placement_id,
        "placement_digest": placement.digest,
        "topology": _topology_to_wire(placement.topology),
        "descriptor": _descriptor_to_wire(placement.descriptor),
        "parts": [_part_to_wire(part) for part in placement.parts],
    }


def _part_to_wire(part: KVCachePlacementPart) -> dict[str, JsonValue]:
    return {
        "resource_id": part.resource_id,
        "revision": part.revision,
        "placement_set_id": part.placement_set_id,
        "topology_id": part.topology_id,
        "participant_id": part.participant_id,
        "rank": _rank_to_wire(part.rank),
        "descriptor": _descriptor_to_wire(part.descriptor),
        "layer_ids": list(part.layer_ids),
        "head_start": part.head_start,
        "head_count": part.head_count,
        "replica_ordinal": part.replica_ordinal,
        "replica_count": part.replica_count,
    }


def _part_from_wire(value: object) -> KVCachePlacementPart:
    part = _require_exact_fields(
        value,
        {
            "resource_id",
            "revision",
            "placement_set_id",
            "topology_id",
            "participant_id",
            "rank",
            "descriptor",
            "layer_ids",
            "head_start",
            "head_count",
            "replica_ordinal",
            "replica_count",
        },
        "KV-cache placement part",
    )
    return KVCachePlacementPart(
        resource_id=ResourceId(_string(part["resource_id"], "resource_id")),
        revision=RevisionId(_string(part["revision"], "revision")),
        placement_set_id=PlacementSetId(
            _string(part["placement_set_id"], "placement_set_id")
        ),
        topology_id=TopologyId(_string(part["topology_id"], "topology_id")),
        participant_id=ParticipantId(_string(part["participant_id"], "participant_id")),
        rank=_rank_from_wire(part["rank"]),
        descriptor=_descriptor_from_wire(part["descriptor"]),
        layer_ids=_integer_tuple(part["layer_ids"], "layer_ids"),
        head_start=_integer(part["head_start"], "head_start"),
        head_count=_integer(part["head_count"], "head_count", minimum=1),
        replica_ordinal=_integer(part["replica_ordinal"], "replica_ordinal"),
        replica_count=_integer(part["replica_count"], "replica_count", minimum=1),
    )


def _topology_to_wire(topology: KVCacheTopology) -> dict[str, JsonValue]:
    return {
        "dp_size": topology.dp_size,
        "pp_size": topology.pp_size,
        "tp_size": topology.tp_size,
        "topology_id": topology.topology_id,
        "participants": [
            {
                "participant_id": item.participant_id,
                "rank": _rank_to_wire(item.rank),
            }
            for item in topology.participants
        ],
    }


def _topology_from_wire(value: object) -> KVCacheTopology:
    topology = _require_exact_fields(
        value,
        {"dp_size", "pp_size", "tp_size", "topology_id", "participants"},
        "KV-cache topology",
    )
    participants: list[KVCacheTopologyParticipant] = []
    for item in _sequence(topology["participants"], "topology participants"):
        participant = _require_exact_fields(
            item, {"participant_id", "rank"}, "topology participant"
        )
        participants.append(
            KVCacheTopologyParticipant(
                participant_id=ParticipantId(
                    _string(participant["participant_id"], "participant_id")
                ),
                rank=_rank_from_wire(participant["rank"]),
            )
        )
    return KVCacheTopology(
        dp_size=_integer(topology["dp_size"], "dp_size", minimum=1),
        pp_size=_integer(topology["pp_size"], "pp_size", minimum=1),
        tp_size=_integer(topology["tp_size"], "tp_size", minimum=1),
        topology_id=TopologyId(_string(topology["topology_id"], "topology_id")),
        participants=tuple(participants),
    )


def _descriptor_to_wire(descriptor: KVCacheDescriptor) -> dict[str, JsonValue]:
    return {
        "global_layer_ids": list(descriptor.global_layer_ids),
        "dtype": descriptor.dtype,
        "itemsize": descriptor.itemsize,
        "page_size": descriptor.page_size,
        "total_kv_heads": descriptor.total_kv_heads,
        "key_head_dim": descriptor.key_head_dim,
        "value_head_dim": descriptor.value_head_dim,
        "layout": descriptor.layout.value,
    }


def _descriptor_from_wire(value: object) -> KVCacheDescriptor:
    descriptor = _require_exact_fields(
        value,
        {
            "global_layer_ids",
            "dtype",
            "itemsize",
            "page_size",
            "total_kv_heads",
            "key_head_dim",
            "value_head_dim",
            "layout",
        },
        "KV-cache descriptor",
    )
    return KVCacheDescriptor(
        global_layer_ids=_integer_tuple(
            descriptor["global_layer_ids"], "global_layer_ids"
        ),
        dtype=_string(descriptor["dtype"], "dtype"),
        itemsize=_integer(descriptor["itemsize"], "itemsize", minimum=1),
        page_size=_integer(descriptor["page_size"], "page_size", minimum=1),
        total_kv_heads=_integer(
            descriptor["total_kv_heads"], "total_kv_heads", minimum=1
        ),
        key_head_dim=_integer(descriptor["key_head_dim"], "key_head_dim", minimum=1),
        value_head_dim=_integer(
            descriptor["value_head_dim"], "value_head_dim", minimum=1
        ),
        layout=KVCacheLayout(_string(descriptor["layout"], "layout")),
    )


def _buffer_to_wire(binding: KVCacheBufferBinding) -> dict[str, JsonValue]:
    fragment = binding.fragment
    return {
        "global_layer_id": binding.global_layer_id,
        "component": binding.component.value,
        "fragment": {
            "placement_fragment_id": fragment.placement_fragment_id,
            "fragment_id": fragment.fragment_id,
            "address": fragment.address,
            "nbytes": fragment.nbytes,
            "worker_id": fragment.worker_id,
            "endpoint": fragment.endpoint,
            "device": fragment.device,
            "itemsize": fragment.itemsize,
            "local_shape": list(fragment.local_shape),
            "strides_bytes": list(fragment.strides_bytes),
            "storage_address": fragment.storage_address,
            "storage_nbytes": fragment.storage_nbytes,
            "storage_offset_bytes": fragment.storage_offset_bytes,
        },
    }


def _buffer_from_wire(value: object) -> KVCacheBufferBinding:
    binding = _require_exact_fields(
        value,
        {"global_layer_id", "component", "fragment"},
        "KV-cache buffer binding",
    )
    fragment = _require_exact_fields(
        binding["fragment"],
        {
            "placement_fragment_id",
            "fragment_id",
            "address",
            "nbytes",
            "worker_id",
            "endpoint",
            "device",
            "itemsize",
            "local_shape",
            "strides_bytes",
            "storage_address",
            "storage_nbytes",
            "storage_offset_bytes",
        },
        "KV-cache runtime buffer",
    )
    return KVCacheBufferBinding(
        global_layer_id=_integer(binding["global_layer_id"], "global_layer_id"),
        component=KVCacheComponent(_string(binding["component"], "component")),
        fragment=KVCacheRuntimeBuffer(
            placement_fragment_id=PlacementFragmentId(
                _string(fragment["placement_fragment_id"], "placement_fragment_id")
            ),
            fragment_id=RuntimeFragmentId(
                _string(fragment["fragment_id"], "fragment_id")
            ),
            address=_integer(fragment["address"], "address", minimum=1),
            nbytes=_integer(fragment["nbytes"], "nbytes", minimum=1),
            worker_id=_string(fragment["worker_id"], "worker_id"),
            endpoint=_string(fragment["endpoint"], "endpoint"),
            device=_string(fragment["device"], "device"),
            itemsize=_integer(fragment["itemsize"], "itemsize", minimum=1),
            local_shape=_integer_tuple(
                fragment["local_shape"], "local_shape", minimum=1
            ),
            strides_bytes=_integer_tuple(fragment["strides_bytes"], "strides_bytes"),
            storage_address=_integer(
                fragment["storage_address"], "storage_address", minimum=1
            ),
            storage_nbytes=_integer(
                fragment["storage_nbytes"], "storage_nbytes", minimum=1
            ),
            storage_offset_bytes=_integer(
                fragment["storage_offset_bytes"], "storage_offset_bytes"
            ),
        ),
    )


def _rank_to_wire(rank: KVCacheRank) -> dict[str, JsonValue]:
    return {"dp": rank.dp, "pp": rank.pp, "tp": rank.tp}


def _rank_from_wire(value: object) -> KVCacheRank:
    rank = _require_exact_fields(value, {"dp", "pp", "tp"}, "KV-cache rank")
    return KVCacheRank(
        dp=_integer(rank["dp"], "rank dp"),
        pp=_integer(rank["pp"], "rank pp"),
        tp=_integer(rank["tp"], "rank tp"),
    )


def _require_resource_kind(payload: JsonObject) -> None:
    if _string(payload["resource_kind"], "resource_kind") != (
        ResourceKind.KV_CACHE.value
    ):
        raise ValueError("resource_kind must be kv_cache")


def _dump(value: JsonValue) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


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
    return _mapping(cast(object, raw), label)


def _mapping(value: object, label: str) -> JsonObject:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a JSON object")  # noqa: TRY004
    mapping = cast(Mapping[object, object], value)
    if any(type(key) is not str for key in mapping):
        raise ValueError(f"{label} must be a JSON object")
    return cast(JsonObject, mapping)


def _require_exact_fields(
    value: object, expected: AbstractSet[str], label: str
) -> JsonObject:
    mapping = _mapping(value, label)
    if set(mapping) != set(expected):
        raise ValueError(f"{label} schema fields do not match contract")
    return mapping


def _sequence(value: object, label: str) -> Sequence[JsonValue]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError(f"{label} must be a sequence")  # noqa: TRY004
    return cast(Sequence[JsonValue], value)


def _string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer at least {minimum}")
    return value


def _integer_tuple(value: object, label: str, *, minimum: int = 0) -> tuple[int, ...]:
    return tuple(
        _integer(item, label, minimum=minimum) for item in _sequence(value, label)
    )


__all__ = [
    "kv_cache_part_from_json",
    "kv_cache_part_to_json",
    "kv_cache_placement_from_json",
    "kv_cache_placement_to_json",
    "kv_cache_runtime_binding_from_json",
    "kv_cache_runtime_binding_to_json",
]
