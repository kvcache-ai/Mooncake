"""Strict JSON for shared Store records; no per-page snapshot serialization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict
from typing import cast

from ..serde import JsonValue, _descriptor_from_wire
from ._utils import canonical_json, load_json_object
from ._utils import json_integer as _integer
from ._utils import json_string as _string
from .manifest import (
    KVCacheStoreFormat,
    KVCacheStoreLayout,
    KVCacheStoreManifest,
    KVCacheStoreShard,
)


def _fields(value: object, names: set[str], label: str) -> Mapping[str, object]:
    if (
        not isinstance(value, Mapping)
        or set(cast(Mapping[object, object], value)) != names
    ):
        raise ValueError(f"{label} fields do not match contract")
    return cast(Mapping[str, object], value)


def _items(value: object, label: str) -> list[object]:
    if type(value) is not list or len(cast(list[object], value)) > 100_000:
        raise ValueError(f"{label} must be an array within the collection limit")
    return cast(list[object], value)


def _load(value: str, fields: set[str], label: str) -> Mapping[str, object]:
    try:
        payload = _fields(load_json_object(value, label), fields, label)
    except RecursionError as error:
        raise ValueError("KV Store JSON nesting limit exceeded") from error
    return payload


def _dump(value: object) -> str:
    encoded = canonical_json(value)
    if len(encoded.encode("utf-8")) > 16 * 1024 * 1024:
        raise ValueError("KV Store JSON exceeds the 16 MiB wire limit")
    return encoded


def kv_cache_store_layout_to_json(layout: KVCacheStoreLayout) -> str:
    """Encode geometry and its canonical identity, with no derived byte fields."""
    if not isinstance(layout, KVCacheStoreLayout):
        raise ValueError("layout must be a KVCacheStoreLayout")  # noqa: TRY004
    return _dump(
        {
            **asdict(layout),
            "layout_id": layout.layout_id,
            "layout_digest": layout.digest,
        }
    )


def kv_cache_store_layout_from_json(value: str) -> KVCacheStoreLayout:
    """Validate exact fields, full shard coverage and canonical digest."""
    payload = _load(
        value,
        {
            "descriptor",
            "object_format",
            "shards",
            "layout_id",
            "layout_digest",
        },
        "KV Store layout",
    )
    shards: list[KVCacheStoreShard] = []
    layer_count = 0
    for item in _items(payload["shards"], "Store shards"):
        raw = _fields(
            item, {"layer_ids", "head_start", "head_count", "key_suffix"}, "Store shard"
        )
        layer_count += len(_items(raw["layer_ids"], "layers"))
        if layer_count * 2 > 100_000:
            raise ValueError("Store fragment count limit exceeded")
        shards.append(
            KVCacheStoreShard(
                tuple(
                    _integer(layer, "layer_id")
                    for layer in _items(raw["layer_ids"], "layers")
                ),
                _integer(raw["head_start"], "head_start"),
                _integer(raw["head_count"], "head_count", minimum=1),
                _string(raw["key_suffix"], "key_suffix"),
            )
        )
    layout = KVCacheStoreLayout(
        _descriptor_from_wire(cast(JsonValue, payload["descriptor"])),
        KVCacheStoreFormat(_string(payload["object_format"], "object_format")),
        tuple(shards),
    )
    if (
        _string(payload["layout_id"], "layout_id"),
        _string(payload["layout_digest"], "layout_digest"),
    ) != (layout.layout_id, layout.digest):
        raise ValueError("stored layout identity does not match content")
    return layout


def kv_cache_store_manifest_to_json(manifest: KVCacheStoreManifest) -> str:
    """Encode one shared model/layout record, not a page or upload attempt."""
    if not isinstance(manifest, KVCacheStoreManifest):
        raise ValueError("manifest must be a KVCacheStoreManifest")  # noqa: TRY004
    return _dump(
        {
            "namespace": manifest.namespace,
            "model_id": manifest.model_id,
            "model_revision": manifest.model_revision,
            "semantic_fingerprint": manifest.semantic_fingerprint,
            "layout": json.loads(kv_cache_store_layout_to_json(manifest.layout)),
            "model_domain": manifest.model_domain,
            "manifest_digest": manifest.digest,
        }
    )


def kv_cache_store_manifest_from_json(value: str) -> KVCacheStoreManifest:
    """Decode a self-contained shared manifest; never query Store or page data."""
    payload = _load(
        value,
        {
            "namespace",
            "model_id",
            "model_revision",
            "semantic_fingerprint",
            "layout",
            "model_domain",
            "manifest_digest",
        },
        "KV Store manifest",
    )
    manifest = KVCacheStoreManifest(
        _string(payload["namespace"], "namespace"),
        _string(payload["model_id"], "model_id"),
        _string(payload["model_revision"], "model_revision"),
        _string(payload["semantic_fingerprint"], "semantic_fingerprint"),
        kv_cache_store_layout_from_json(_dump(payload["layout"])),
    )
    if manifest.model_domain != _string(payload["model_domain"], "model_domain"):
        raise ValueError("model domain does not match content")
    if manifest.digest != _string(payload["manifest_digest"], "manifest_digest"):
        raise ValueError("stored manifest digest does not match content")
    return manifest


__all__ = [
    "kv_cache_store_layout_from_json",
    "kv_cache_store_layout_to_json",
    "kv_cache_store_manifest_from_json",
    "kv_cache_store_manifest_to_json",
]
