from __future__ import annotations

import copy
import logging
import threading
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any


logger = logging.getLogger(__name__)


class DataProtoCatalog:
    """Map logical rows and fields to immutable DataProto refs.

    Calls to one catalog instance must be serialized by its host. Direct
    handles remain metadata-only. DataProtoCatalogTransfer marks only fresh,
    independent fragments as managed so their physical lifetime, including
    append-derived handles, can follow logical references and active readers.
    """

    def __init__(self) -> None:
        self._partitions: dict[str, dict[str, dict[str, Any]]] = {}
        self._fragments: dict[str, dict[str, Any]] = {}
        self._retired: dict[str, dict[str, Any]] = {}
        self._publications: dict[str, dict[str, Any]] = {}
        self._fragment_refcounts: dict[str, int] = {}
        self._managed_fragments: set[str] = set()
        self._fragment_readers: dict[str, int] = {}
        self._read_pins: dict[int, tuple[str, ...]] = {}
        self._next_read_token = 0
        self._drained = False

    def update(
        self,
        partition: str,
        keys: Sequence[str],
        *,
        tags: Sequence[Mapping[str, Any]] | None = None,
        handle: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._update(partition, keys, tags=tags, handle=handle, managed=False)

    def publish(
        self,
        operation_id: str,
        partition: str,
        keys: Sequence[str],
        *,
        tags: Sequence[Mapping[str, Any]] | None = None,
        handle: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Idempotently publish one transfer-managed fresh fragment."""
        operation_id = _nonempty_string(operation_id, "operation_id")
        previous = self._publications.get(operation_id)
        if previous is not None:
            return copy.deepcopy(previous)

        result = self._update(
            partition,
            keys,
            tags=tags,
            handle=handle,
            managed=handle is not None,
        )
        self._publications[operation_id] = copy.deepcopy(result)
        return result

    def ack_publication(self, operation_id: str) -> None:
        self._publications.pop(_nonempty_string(operation_id, "operation_id"), None)

    def publish_append(
        self,
        operation_id: str,
        fragment_id: str,
        partition: str,
        keys: Sequence[str],
        *,
        previous_handle: Mapping[str, Any],
        handle: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Idempotently replace a managed fragment with its appended handle."""
        operation_id = _nonempty_string(operation_id, "operation_id")
        previous = self._publications.get(operation_id)
        if previous is not None:
            return copy.deepcopy(previous)
        if self._drained:
            return self._reject_append(operation_id, "DataProto catalog is drained")
        fragment_id = _nonempty_string(fragment_id, "fragment_id")
        partition = _nonempty_string(partition, "partition")
        keys = _names(keys, "keys")
        previous_ref, normalized_previous = _normalize_handle(previous_handle)
        ref, normalized = _normalize_handle(handle)
        current = self._fragments.get(fragment_id)
        if (
            current is None
            or fragment_id not in self._managed_fragments
            or current != normalized_previous
        ):
            return self._reject_append(
                operation_id, f"stale DataProto append handle: {fragment_id!r}"
            )
        if (
            ref.batch_size != previous_ref.batch_size
            or ref.batch_size != len(keys)
            or ref.partition != previous_ref.partition
            or ref.partition != partition
            or ref._storage_group_id != previous_ref._storage_group_id
        ):
            return self._reject_append(
                operation_id, "appended DataProto handle changed fragment identity"
            )
        previous_fields = previous_ref.field_index
        if any(
            ref.field_index.get(name) != location
            for name, location in previous_fields.items()
        ):
            return self._reject_append(
                operation_id, "appended DataProto handle changed existing fields"
            )
        new_fields = [
            field for field in ref.field_index if field not in previous_fields
        ]

        entries = self._partitions.get(partition, {})
        missing = [key for key in keys if key not in entries]
        if missing:
            return self._reject_append(
                operation_id,
                f"keys not found in partition {partition!r}: {missing}",
            )
        for row, key in enumerate(keys):
            for field in new_fields:
                location = (fragment_id, row)
                old_location = entries[key]["fields"].get(field)
                if old_location is not None:
                    self._drop_fragment_reference(old_location[0])
                entries[key]["fields"][field] = location
                self._fragment_refcounts[fragment_id] += 1
        self._fragments[fragment_id] = normalized
        result = {
            "keys": list(keys),
            "tags": [copy.deepcopy(entries[key]["tag"]) for key in keys],
            "fields": _field_union(entries, keys),
            **self._retired_result(),
        }
        self._publications[operation_id] = copy.deepcopy(result)
        return result

    def _reject_append(self, operation_id: str, message: str) -> dict[str, Any]:
        result = {"append_rejected": message}
        self._publications[operation_id] = copy.deepcopy(result)
        return result

    def _update(
        self,
        partition: str,
        keys: Sequence[str],
        *,
        tags: Sequence[Mapping[str, Any]] | None,
        handle: Mapping[str, Any] | None,
        managed: bool,
    ) -> dict[str, Any]:
        if self._drained:
            raise RuntimeError("DataProto catalog is drained")
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
            if (
                previous is not None
                and (fragment_id in self._managed_fragments) != managed
            ):
                raise ValueError(
                    f"DataProto fragment management mismatch: {fragment_id!r}"
                )
            if fragment_id in self._retired:
                raise ValueError(
                    f"DataProto fragment id is pending retirement: {fragment_id!r}"
                )

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
            if managed:
                self._managed_fragments.add(fragment_id)

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
            **self._retired_result(),
        }

    def resolve(
        self,
        partition: str,
        keys: Sequence[str],
        fields: Sequence[str] | None = None,
        *,
        pin: bool = False,
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

        handles = {
            fragment_id: copy.deepcopy(self._fragments[fragment_id])
            for fragment_id in fragment_ids
        }
        meta_info: dict[str, Any] = {}
        for handle in handles.values():
            for name, value in handle.get("meta_info", {}).items():
                if name in meta_info and meta_info[name] != value:
                    raise ValueError(
                        f"conflicting DataProto meta_info value for {name!r}"
                    )
                meta_info[name] = copy.deepcopy(value)

        result = {
            "keys": list(keys),
            "tags": [copy.deepcopy(entries[key]["tag"]) for key in keys],
            "fields": selected,
            "field_groups": [
                {"fields": fields, "locations": list(locations)}
                for locations, fields in grouped_fields.items()
            ],
            "handles": handles,
            "meta_info": meta_info,
        }
        if pin:
            self._next_read_token += 1
            token = self._next_read_token
            pinned = tuple(fragment_ids)
            self._read_pins[token] = pinned
            for fragment_id in pinned:
                self._fragment_readers[fragment_id] = (
                    self._fragment_readers.get(fragment_id, 0) + 1
                )
            result["read_token"] = token
        return result

    def release_read(self, token: int) -> dict[str, Any]:
        """Idempotently release a pinned resolve and return pending handles."""
        fragment_ids = self._read_pins.pop(token, None)
        if fragment_ids is None:
            return self._retired_result()

        for fragment_id in fragment_ids:
            readers = self._fragment_readers[fragment_id] - 1
            if readers:
                self._fragment_readers[fragment_id] = readers
                continue
            del self._fragment_readers[fragment_id]
            if fragment_id not in self._fragment_refcounts:
                self._retire_fragment(fragment_id)
        return self._retired_result()

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

    def remove(self, partition: str, keys: Sequence[str]) -> dict[str, Any]:
        """Remove logical keys after their writers have finished.

        Returned managed handles can be passed to ``cleanup_dataproto``.
        """
        partition = _nonempty_string(partition, "partition")
        keys = _names(keys, "keys")
        entries = self._partitions.get(partition)
        if entries is None:
            return self._retired_result()

        for key in keys:
            entry = entries.pop(key, None)
            if entry is None:
                continue
            for fragment_id, _row in entry["fields"].values():
                self._drop_fragment_reference(fragment_id)
        if not entries:
            del self._partitions[partition]
        return self._retired_result()

    def drain(self) -> dict[str, Any]:
        """Retire all metadata after clients quiesce and return pending handles."""
        if self._read_pins:
            raise RuntimeError(
                f"cannot drain DataProto catalog with {len(self._read_pins)} active read(s)"
            )
        for fragment_id in self._managed_fragments:
            self._retired[fragment_id] = self._fragments[fragment_id]
        self._partitions.clear()
        self._fragments.clear()
        self._fragment_refcounts.clear()
        self._managed_fragments.clear()
        self._fragment_readers.clear()
        self._publications.clear()
        self._drained = True
        return self._retired_result()

    def ack_retired(self, handles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        """Forget retired handles whose Store objects were removed successfully."""
        for handle in handles:
            ref, normalized = _normalize_handle(handle)
            fragment_id = next(
                (
                    key
                    for key, pending in self._retired.items()
                    if pending == normalized
                ),
                None,
            )
            if fragment_id is not None:
                del self._retired[fragment_id]
                continue
            if len(ref.stage_refs) == 1:
                fragment_id = next(iter(ref.stage_refs.values())).manifest_key
                if fragment_id in self._retired:
                    raise ValueError(
                        f"retired DataProto fragment mismatch: {fragment_id!r}"
                    )
        return self._retired_result()

    def _drop_fragment_reference(self, fragment_id: str) -> None:
        remaining = self._fragment_refcounts[fragment_id] - 1
        if remaining:
            self._fragment_refcounts[fragment_id] = remaining
            return
        del self._fragment_refcounts[fragment_id]
        if self._fragment_readers.get(fragment_id, 0):
            return
        self._retire_fragment(fragment_id)

    def _retire_fragment(self, fragment_id: str) -> None:
        handle = self._fragments.pop(fragment_id)
        if fragment_id in self._managed_fragments:
            self._managed_fragments.remove(fragment_id)
            self._retired[fragment_id] = handle

    def _retired_result(self) -> dict[str, Any]:
        return {
            "retired_handles": [
                copy.deepcopy(handle) for handle in self._retired.values()
            ]
        }


class DataProtoCatalogTransfer:
    _RESULTS_ATTR = "_mooncake_catalog_results"

    def __init__(self, transfer: Any, catalog_call: Callable[..., Any]) -> None:
        self.transfer = transfer
        self._catalog_call = catalog_call
        self._pending_publications: list[dict[str, Any]] = []
        self._pending_publication_acks: list[str] = []
        self._pending_results: list[Any] = []
        self._cleanup_lock = threading.RLock()

    def put(
        self,
        data: Any,
        *,
        partition: str,
        keys: Sequence[str],
        tags: Sequence[Mapping[str, Any]] | None = None,
        **put_kwargs: Any,
    ) -> dict[str, Any]:
        """Put one immutable fragment and atomically publish it in Catalog."""
        self._retry_publication_acks()
        self._retry_publications(strict=True)
        partition = _nonempty_string(partition, "partition")
        normalized_keys = _names(keys, "keys")
        normalized_tags = _tags(tags, len(normalized_keys))
        if data is None and normalized_tags is None:
            raise ValueError("catalog update requires tags or DataProto data")

        handle = None
        if data is not None:
            if len(data) != len(normalized_keys):
                raise ValueError(
                    f"DataProto batch size {len(data)} does not match {len(normalized_keys)} logical keys"
                )
            from mooncake.structured_object_store import export_dataproto_ref

            handle = export_dataproto_ref(
                self.transfer.put(
                    data, type="dataproto", partition=partition, **put_kwargs
                )
            )
        publication = {
            "operation_id": uuid.uuid4().hex,
            "method": "publish",
            "args": (partition, normalized_keys),
            "kwargs": {"tags": normalized_tags, "handle": handle},
        }
        for attempt in range(2):
            try:
                return self._publish(publication)
            except Exception:
                if attempt:
                    with self._cleanup_lock:
                        self._pending_publications.append(publication)
                    raise
                logger.exception("Retrying DataProto Catalog publication")
        raise AssertionError("unreachable")

    def append(
        self,
        fragment_id: str,
        handle: Mapping[str, Any],
        data: Any,
        *,
        partition: str,
        keys: Sequence[str],
        stage: str,
        **append_kwargs: Any,
    ) -> dict[str, Any]:
        """Append fields and transfer managed-fragment ownership to the result."""
        self._retry_publication_acks()
        self._retry_publications(strict=True)
        fragment_id = _nonempty_string(fragment_id, "fragment_id")
        partition = _nonempty_string(partition, "partition")
        normalized_keys = _names(keys, "keys")
        if append_kwargs.get("overwrite"):
            raise ValueError("catalog append does not support overwrite")
        if len(data) != len(normalized_keys):
            raise ValueError(
                f"DataProto batch size {len(data)} does not match {len(normalized_keys)} logical keys"
            )
        previous_ref, previous_handle = _normalize_handle(handle)
        if previous_ref.partition != partition:
            raise ValueError(
                "DataProto handle partition does not match Catalog partition"
            )
        plan = self._catalog_call(
            "resolve", partition, normalized_keys, list(previous_ref.field_index)
        )
        if plan["handles"].get(fragment_id) != previous_handle:
            raise ValueError(f"stale DataProto append handle: {fragment_id!r}")
        from mooncake.structured_object_store import export_dataproto_ref

        appended_handle = export_dataproto_ref(
            self.transfer.append_dataproto_fields(
                previous_handle, data, stage=stage, **append_kwargs
            )
        )
        publication = {
            "operation_id": uuid.uuid4().hex,
            "method": "publish_append",
            "args": (fragment_id, partition, normalized_keys),
            "kwargs": {
                "previous_handle": previous_handle,
                "handle": appended_handle,
            },
        }
        for attempt in range(2):
            try:
                result = self._publish(publication)
            except Exception:
                if attempt:
                    with self._cleanup_lock:
                        self._pending_publications.append(publication)
                    raise
                logger.exception("Retrying DataProto Catalog append publication")
            else:
                rejection = result.get("append_rejected")
                if rejection is not None:
                    raise ValueError(rejection)
                return result
        raise AssertionError("unreachable")

    def resolve(
        self,
        partition: str,
        keys: Sequence[str],
        fields: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        return self._catalog_call("resolve", partition, keys, fields, pin=True)

    def release_read(self, token: int) -> None:
        self._cleanup_retired(self._catalog_call("release_read", token))

    def attach_results(self, output: Any, results: Sequence[Any]) -> None:
        setattr(output, self._RESULTS_ATTR, list(results))

    def release_result(self, output: Any) -> None:
        results = getattr(output, self._RESULTS_ATTR, ()) or ()
        setattr(output, self._RESULTS_ATTR, [])
        self.discard_results(results)

    def discard_results(self, results: Sequence[Any], *, strict: bool = False) -> None:
        self._retry_pending(
            "_pending_results",
            results,
            self.transfer.release_result,
            "release DataProto read buffer",
            attempts=2,
            strict=strict,
        )

    def list(self, partition: str | None = None) -> dict[str, dict[str, Any]]:
        return self._catalog_call("list", partition)

    def remove(self, partition: str, keys: Sequence[str]) -> None:
        self._cleanup_retired(self._catalog_call("remove", partition, keys))

    def drain(self) -> None:
        for attempt in range(2):
            try:
                self._retry_publications(strict=True)
                self._cleanup_retired(self._catalog_call("drain"), strict=True)
                return
            except Exception:
                if attempt:
                    raise
                logger.exception("Retrying DataProto Catalog drain")

    def close(self) -> None:
        self.discard_results((), strict=True)
        self._retry_publications(strict=True)
        self._retry_publication_acks(strict=True)

    def _cleanup_retired(
        self, result: Mapping[str, Any], *, strict: bool = False
    ) -> None:
        removed = []
        failures = 0
        for handle in result.get("retired_handles", ()):
            try:
                self.transfer.cleanup_dataproto(handle)
            except Exception:
                failures += 1
                logger.exception("Failed to clean up retired DataProto fragment")
            else:
                removed.append(handle)
        if removed:
            try:
                self._catalog_call("ack_retired", removed)
            except Exception:
                logger.exception("Failed to acknowledge retired DataProto fragments")
                if strict:
                    raise
        if strict and failures:
            raise RuntimeError(
                f"failed to clean up {failures} retired DataProto fragment(s)"
            )

    def _publish(self, publication: Mapping[str, Any]) -> dict[str, Any]:
        operation_id = publication["operation_id"]
        result = self._catalog_call(
            publication["method"],
            operation_id,
            *publication["args"],
            **publication["kwargs"],
        )
        if publication["method"] == "publish_append" and result.get("append_rejected"):
            self.transfer.cleanup_dataproto_append(
                publication["kwargs"]["previous_handle"],
                publication["kwargs"]["handle"],
            )
        self._cleanup_retired(result)
        self._ack_publication(operation_id)
        return result

    def _ack_publication(self, operation_id: str) -> None:
        try:
            self._catalog_call("ack_publication", operation_id)
        except Exception:
            logger.exception("Failed to acknowledge DataProto Catalog publication")
            with self._cleanup_lock:
                if operation_id not in self._pending_publication_acks:
                    self._pending_publication_acks.append(operation_id)

    def _retry_publication_acks(self, *, strict: bool = False) -> None:
        self._retry_pending(
            "_pending_publication_acks",
            (),
            lambda operation_id: self._catalog_call("ack_publication", operation_id),
            "acknowledge DataProto Catalog publication",
            strict=strict,
        )

    def _retry_publications(self, *, strict: bool = False) -> None:
        self._retry_pending(
            "_pending_publications",
            (),
            self._publish,
            "publish pending DataProto fragment",
            strict=strict,
        )

    def _retry_pending(
        self,
        pending_attr: str,
        items: Sequence[Any],
        action: Callable[[Any], None],
        label: str,
        *,
        attempts: int = 1,
        strict: bool = False,
    ) -> None:
        with self._cleanup_lock:
            pending = [*getattr(self, pending_attr), *items]
            setattr(self, pending_attr, [])
        for _attempt in range(attempts):
            failed = []
            for item in pending:
                try:
                    action(item)
                except Exception:
                    logger.exception("Failed to %s", label)
                    failed.append(item)
            pending = failed
        with self._cleanup_lock:
            getattr(self, pending_attr).extend(pending)
            pending_count = len(getattr(self, pending_attr))
        if strict and pending_count:
            raise RuntimeError(f"failed to {label} for {pending_count} item(s)")


def _normalize_handle(
    handle: Mapping[str, Any],
) -> tuple[Any, dict[str, Any]]:
    from mooncake.structured_object_store import (
        export_dataproto_ref,
        import_dataproto_ref,
    )

    ref = import_dataproto_ref(handle)
    return ref, export_dataproto_ref(ref)


def _fragment(
    handle: Mapping[str, Any], batch_size: int
) -> tuple[str, dict[str, Any], tuple[str, ...]]:
    ref, stored_handle = _normalize_handle(handle)
    if len(ref.stage_refs) != 1:
        raise ValueError("catalog fragments must contain exactly one DataProto stage")
    fragment_id = _nonempty_string(
        next(iter(ref.stage_refs.values())).manifest_key,
        "manifest_key",
    )
    if ref.batch_size != batch_size:
        raise ValueError(
            f"DataProto batch size {ref.batch_size!r} does not match "
            f"{batch_size} logical keys"
        )
    fields = _names(ref.field_index, "field names")
    if not fields:
        raise ValueError("DataProto catalog fragments cannot be empty")
    return fragment_id, stored_handle, fields


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
