"""Strict JSON boundary for KV-cache snapshot descriptors."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

from ..contracts import ResourceId, ResourceKind
from .snapshot import KVCacheSnapshotDescriptor, SnapshotId


def kv_cache_snapshot_to_json(snapshot: KVCacheSnapshotDescriptor) -> str:
    if not isinstance(snapshot, KVCacheSnapshotDescriptor):
        raise ValueError("snapshot must be a KVCacheSnapshotDescriptor")  # noqa: TRY004
    return json.dumps(
        {
            "resource_kind": snapshot.resource_kind.value,
            "namespace": snapshot.namespace,
            "resource_id": snapshot.resource_id,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_digest": snapshot.digest,
            "model_id": snapshot.model_id,
            "model_revision": snapshot.model_revision,
            "token_start": snapshot.token_start,
            "token_count": snapshot.token_count,
            "token_fingerprint": snapshot.token_fingerprint,
            "semantic_fingerprint": snapshot.semantic_fingerprint,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def kv_cache_snapshot_from_json(value: str) -> KVCacheSnapshotDescriptor:
    payload = _load_json_object(value)
    expected = {
        "resource_kind",
        "namespace",
        "resource_id",
        "snapshot_id",
        "snapshot_digest",
        "model_id",
        "model_revision",
        "token_start",
        "token_count",
        "token_fingerprint",
        "semantic_fingerprint",
    }
    if set(payload) != expected:
        raise ValueError("KV-cache snapshot fields do not match contract")
    if (
        _string(payload["resource_kind"], "resource_kind")
        != ResourceKind.KV_CACHE.value
    ):
        raise ValueError("resource_kind must be kv_cache")
    snapshot = KVCacheSnapshotDescriptor(
        namespace=_string(payload["namespace"], "namespace"),
        resource_id=ResourceId(_string(payload["resource_id"], "resource_id")),
        snapshot_id=SnapshotId(_string(payload["snapshot_id"], "snapshot_id")),
        model_id=_string(payload["model_id"], "model_id"),
        model_revision=_string(payload["model_revision"], "model_revision"),
        token_start=_integer(payload["token_start"], "token_start"),
        token_count=_integer(payload["token_count"], "token_count", minimum=1),
        token_fingerprint=_string(payload["token_fingerprint"], "token_fingerprint"),
        semantic_fingerprint=_string(
            payload["semantic_fingerprint"], "semantic_fingerprint"
        ),
    )
    if snapshot.digest != _string(payload["snapshot_digest"], "snapshot_digest"):
        raise ValueError("KV-cache snapshot digest does not match content")
    return snapshot


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
        raise ValueError("KV-cache snapshot is not valid JSON") from error
    if not isinstance(payload, Mapping):
        raise ValueError("KV-cache snapshot must be an object")  # noqa: TRY004
    return cast(Mapping[str, object], payload)


def _string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer of at least {minimum}")
    return value


__all__ = ["kv_cache_snapshot_from_json", "kv_cache_snapshot_to_json"]
