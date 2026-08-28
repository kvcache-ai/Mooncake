"""Strict JSON boundary for address-free KV-cache logical plans."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import cast

from ..contracts import ParticipantId
from .planner import KVCacheLogicalTransferPlan, KVCacheTransferEdge
from .serde import kv_cache_placement_from_json, kv_cache_placement_to_json
from .snapshot_serde import kv_cache_snapshot_from_json, kv_cache_snapshot_to_json
from .types import KVCacheComponent


def kv_cache_logical_plan_to_json(plan: KVCacheLogicalTransferPlan) -> str:
    if not isinstance(plan, KVCacheLogicalTransferPlan):
        raise ValueError("plan must be a KVCacheLogicalTransferPlan")  # noqa: TRY004
    return json.dumps(
        {
            "source_placement_json": kv_cache_placement_to_json(plan.source_placement),
            "target_placement_json": kv_cache_placement_to_json(plan.target_placement),
            "target_participant_id": plan.target_participant_id,
            "snapshot_json": (
                kv_cache_snapshot_to_json(plan.snapshot)
                if plan.snapshot is not None
                else None
            ),
            "plan_id": plan.plan_id,
            "plan_digest": plan.digest,
            "edges": [_edge_to_wire(edge) for edge in plan.edges],
            "expected_writer_ids": list(plan.expected_writer_ids),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def kv_cache_logical_plan_from_json(value: str) -> KVCacheLogicalTransferPlan:
    payload = _load_json_object(value)
    expected = {
        "source_placement_json",
        "target_placement_json",
        "target_participant_id",
        "snapshot_json",
        "plan_id",
        "plan_digest",
        "edges",
        "expected_writer_ids",
    }
    if set(payload) != expected:
        raise ValueError("KV-cache logical plan fields do not match contract")
    snapshot_json = _optional_string(payload["snapshot_json"], "snapshot_json")
    result = KVCacheLogicalTransferPlan(
        source_placement=kv_cache_placement_from_json(
            _string(payload["source_placement_json"], "source_placement_json")
        ),
        target_placement=kv_cache_placement_from_json(
            _string(payload["target_placement_json"], "target_placement_json")
        ),
        target_participant_id=ParticipantId(
            _string(payload["target_participant_id"], "target_participant_id")
        ),
        edges=tuple(
            _edge_from_wire(item)
            for item in _sequence(payload["edges"], "logical plan edges")
        ),
        expected_writer_ids=tuple(
            ParticipantId(_string(item, "expected_writer_id"))
            for item in _sequence(payload["expected_writer_ids"], "expected_writer_ids")
        ),
        snapshot=(
            kv_cache_snapshot_from_json(snapshot_json)
            if snapshot_json is not None
            else None
        ),
    )
    if result.plan_id != _string(payload["plan_id"], "plan_id"):
        raise ValueError("KV-cache logical plan ID does not match semantic content")
    if result.digest != _string(payload["plan_digest"], "plan_digest"):
        raise ValueError("KV-cache logical plan digest does not match content")
    return result


def _load_json_object(value: str) -> Mapping[str, object]:
    def reject_constant(constant: str) -> None:
        raise ValueError(f"non-finite JSON number is unsupported: {constant}")

    def reject_duplicate_fields(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON field: {key}")
            result[key] = item
        return result

    try:
        payload = json.loads(
            value,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate_fields,
        )
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("KV-cache logical plan is not valid JSON") from error
    return _mapping(payload, "KV-cache logical plan")


def _edge_to_wire(edge: KVCacheTransferEdge) -> dict[str, object]:
    return {
        "source_participant_id": edge.source_participant_id,
        "target_participant_id": edge.target_participant_id,
        "global_layer_id": edge.global_layer_id,
        "component": edge.component.value,
        "global_head_start": edge.global_head_start,
        "head_count": edge.head_count,
        "source_head_offset": edge.source_head_offset,
        "target_head_offset": edge.target_head_offset,
        "head_dim": edge.head_dim,
        "itemsize": edge.itemsize,
    }


def _edge_from_wire(value: object) -> KVCacheTransferEdge:
    edge = _mapping(value, "KV-cache transfer edge")
    expected = {
        "source_participant_id",
        "target_participant_id",
        "global_layer_id",
        "component",
        "global_head_start",
        "head_count",
        "source_head_offset",
        "target_head_offset",
        "head_dim",
        "itemsize",
    }
    if set(edge) != expected:
        raise ValueError("KV-cache transfer edge fields do not match contract")
    return KVCacheTransferEdge(
        source_participant_id=ParticipantId(
            _string(edge["source_participant_id"], "source_participant_id")
        ),
        target_participant_id=ParticipantId(
            _string(edge["target_participant_id"], "target_participant_id")
        ),
        global_layer_id=_integer(edge["global_layer_id"], "global_layer_id"),
        component=KVCacheComponent(_string(edge["component"], "component")),
        global_head_start=_integer(edge["global_head_start"], "global_head_start"),
        head_count=_integer(edge["head_count"], "head_count"),
        source_head_offset=_integer(edge["source_head_offset"], "source_head_offset"),
        target_head_offset=_integer(edge["target_head_offset"], "target_head_offset"),
        head_dim=_integer(edge["head_dim"], "head_dim"),
        itemsize=_integer(edge["itemsize"], "itemsize"),
    )


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")  # noqa: TRY004
    return cast(Mapping[str, object], value)


def _sequence(value: object, label: str) -> Sequence[object]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError(f"{label} must be a sequence")  # noqa: TRY004
    return cast(Sequence[object], value)


def _string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _optional_string(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _string(value, label)


def _integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


__all__ = ["kv_cache_logical_plan_from_json", "kv_cache_logical_plan_to_json"]
