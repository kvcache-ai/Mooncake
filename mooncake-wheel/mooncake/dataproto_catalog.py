from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Any


class DataProtoCatalog:
    """Map logical rows and fields to immutable DataProto refs.

    The catalog only tracks metadata. Data transfer and object lifetime remain
    owned by MooncakeBundleTransfer and Mooncake Store, respectively. Calls to
    one catalog instance must be serialized by its host. Each update publishes
    one immutable, single-stage fragment; multi-stage append handles are not
    catalog fragments.
    """

    def __init__(self) -> None:
        self._partitions: dict[str, dict[str, dict[str, Any]]] = {}
        self._fragments: dict[str, dict[str, Any]] = {}
        self._fragment_refcounts: dict[str, int] = {}

    def update(
        self,
        partition: str,
        keys: Sequence[str],
        *,
        tags: Sequence[Mapping[str, Any]] | None = None,
        handle: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Merge tags and publish every field in a single-stage ``handle``."""
        partition = _nonempty_string(partition, "partition")
        keys = _names(keys, "keys")
        normalized_tags = _tags(tags, len(keys))
        fragment = _fragment(handle, len(keys)) if handle is not None else None
        if normalized_tags is None and fragment is None:
            raise ValueError("catalog update requires tags or a DataProto handle")

        if fragment is not None:
            fragment_id, stored_handle, fields = fragment
            previous = self._fragments.get(fragment_id)
            if previous is not None and previous != stored_handle:
                raise ValueError(f"DataProto fragment id collision: {fragment_id!r}")

        current_entries = self._partitions.get(partition, {})
        merged_tags = {}
        response_tags = []
        for row, key in enumerate(keys):
            tag = copy.deepcopy(current_entries.get(key, {"tag": {}})["tag"])
            if normalized_tags is not None:
                tag.update(normalized_tags[row])
            merged_tags[key] = tag
            response_tags.append(copy.deepcopy(tag))

        entries = self._partitions.setdefault(partition, {})
        if fragment is not None:
            self._fragments.setdefault(fragment_id, stored_handle)
            self._fragment_refcounts.setdefault(fragment_id, 0)

        for row, key in enumerate(keys):
            entry = entries.setdefault(key, {"tag": {}, "fields": {}})
            entry["tag"] = merged_tags[key]
            if fragment is None:
                continue
            for field in fields:
                location = (fragment_id, row)
                old_location = entry["fields"].get(field)
                if old_location == location:
                    continue
                if old_location is not None and old_location[0] == fragment_id:
                    entry["fields"][field] = location
                    continue
                if old_location is not None:
                    self._drop_fragment_reference(old_location[0])
                entry["fields"][field] = location
                self._fragment_refcounts[fragment_id] += 1

        return {
            "keys": list(keys),
            "tags": response_tags,
            "fields": _field_union(entries, keys),
        }

    def resolve(
        self,
        partition: str,
        keys: Sequence[str],
        fields: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Resolve an ordered logical read into immutable fragment locations."""
        partition = _nonempty_string(partition, "partition")
        keys = _names(keys, "keys")
        entries = self._partitions.get(partition, {})
        missing = [key for key in keys if key not in entries]
        if missing:
            raise ValueError(
                f"keys were not found in partition {partition!r}: {missing}"
            )

        selected = (
            list(_names(fields, "field names"))
            if fields is not None
            else _field_union(entries, keys)
        )
        if not selected:
            raise ValueError("requested keys do not contain any fields")

        for key in keys:
            entry_fields = entries[key]["fields"]
            unavailable = [field for field in selected if field not in entry_fields]
            if unavailable:
                raise ValueError(f"fields are not ready for key {key!r}: {unavailable}")

        grouped_fields: dict[tuple[tuple[str, int], ...], list[str]] = {}
        fragment_ids: set[str] = set()
        for field in selected:
            locations = tuple(entries[key]["fields"][field] for key in keys)
            grouped_fields.setdefault(locations, []).append(field)
            fragment_ids.update(location[0] for location in locations)

        return {
            "keys": list(keys),
            "tags": [copy.deepcopy(entries[key]["tag"]) for key in keys],
            "fields": selected,
            "field_groups": [
                {"fields": fields, "locations": list(locations)}
                for locations, fields in grouped_fields.items()
            ],
            "handles": {
                fragment_id: copy.deepcopy(self._fragments[fragment_id])
                for fragment_id in fragment_ids
            },
        }

    def list(self, partition: str | None = None) -> dict[str, dict[str, Any]]:
        """Return tags using the same partition/key shape as rollout KV APIs."""
        if partition is not None:
            partition = _nonempty_string(partition, "partition")
            partitions = [partition] if partition in self._partitions else []
        else:
            partitions = list(self._partitions)
        return {
            partition_id: {
                key: copy.deepcopy(entry["tag"])
                for key, entry in self._partitions[partition_id].items()
            }
            for partition_id in partitions
        }

    def remove(self, partition: str, keys: Sequence[str]) -> None:
        """Remove logical keys after their writers have finished.

        This only removes catalog metadata; physical lifetime stays with Store.
        """
        partition = _nonempty_string(partition, "partition")
        keys = _names(keys, "keys")
        entries = self._partitions.get(partition)
        if entries is None:
            return

        for key in keys:
            entry = entries.pop(key, None)
            if entry is None:
                continue
            for fragment_id, _row in entry["fields"].values():
                self._drop_fragment_reference(fragment_id)
        if not entries:
            del self._partitions[partition]

    def _drop_fragment_reference(self, fragment_id: str) -> None:
        remaining = self._fragment_refcounts[fragment_id] - 1
        if remaining:
            self._fragment_refcounts[fragment_id] = remaining
            return
        del self._fragment_refcounts[fragment_id]
        del self._fragments[fragment_id]


def _fragment(
    handle: Mapping[str, Any], batch_size: int
) -> tuple[str, dict[str, Any], tuple[str, ...]]:
    from mooncake.structured_object_store import (
        export_dataproto_ref,
        import_dataproto_ref,
    )

    ref = import_dataproto_ref(handle)
    if ref.batch_size != batch_size:
        raise ValueError(
            f"DataProto batch size {ref.batch_size!r} does not match "
            f"{batch_size} logical keys"
        )
    if len(ref.stage_refs) != 1:
        raise ValueError("catalog fragments must contain exactly one DataProto stage")
    fragment_id = _nonempty_string(
        next(iter(ref.stage_refs.values())).manifest_key, "manifest_key"
    )
    fields = _names(ref.field_index, "field names")
    if not fields:
        raise ValueError("DataProto catalog fragments cannot be empty")
    return fragment_id, export_dataproto_ref(ref), fields


def _field_union(
    entries: Mapping[str, Mapping[str, Any]], keys: Sequence[str]
) -> list[str]:
    return list(
        dict.fromkeys(field for key in keys for field in entries[key]["fields"])
    )


def _names(values: Sequence[str] | Mapping[str, Any], label: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise ValueError(f"{label} must be a sequence of non-empty strings")
    result = tuple(values)
    if not result or any(not isinstance(value, str) or not value for value in result):
        raise ValueError(f"{label} must be non-empty strings")
    if len(result) != len(set(result)):
        raise ValueError(f"{label} must be unique")
    return result


def _tags(
    tags: Sequence[Mapping[str, Any]] | None, count: int
) -> list[dict[str, Any]] | None:
    if tags is None:
        return None
    if len(tags) != count:
        raise ValueError("tags must have the same length as keys")
    if any(not isinstance(tag, Mapping) for tag in tags):
        raise TypeError("tags must be mappings")
    return [copy.deepcopy(dict(tag)) for tag in tags]


def _nonempty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value
