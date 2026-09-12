from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from mooncake.dataproto_catalog import DataProtoCatalog, DataProtoCatalogTransfer
from mooncake.structured_object_store import import_dataproto_ref


def handle(name: str, fields: list[str], batch_size: int = 2) -> dict:
    return {
        "type": "mooncake_dataproto_ref",
        "version": 1,
        "kind": "bundle_stages",
        "batch_size": batch_size,
        "stage_refs": {"rollout": {"manifest_key": f"manifest/{name}"}},
        "field_index": {
            field: {
                "stage": "rollout",
                "member": f"batch.{field}",
                "section": "batch",
            }
            for field in fields
        },
    }


def test_catalog_merges_tags_and_resolves_fragmented_fields_in_key_order() -> None:
    catalog = DataProtoCatalog()
    catalog.update(
        "train",
        ["a", "b"],
        tags=[{"status": "running"}, {"status": "running"}],
        handle=handle("base", ["input_ids", "attention_mask"]),
    )
    result = catalog.update(
        "train",
        ["b", "a"],
        tags=[{"status": "done"}, {"status": "done"}],
        handle=handle("scores", ["score"]),
    )

    assert result["fields"] == ["input_ids", "attention_mask", "score"]
    assert catalog.list("train") == {
        "train": {
            "a": {"status": "done"},
            "b": {"status": "done"},
        }
    }
    plan = catalog.resolve("train", ["a", "b"], ["score", "input_ids"])
    assert plan["fields"] == ["score", "input_ids"]
    assert plan["field_groups"] == [
        {
            "fields": ["score"],
            "locations": [("manifest/scores", 1), ("manifest/scores", 0)],
        },
        {
            "fields": ["input_ids"],
            "locations": [("manifest/base", 0), ("manifest/base", 1)],
        },
    ]
    assert set(plan["handles"]) == {"manifest/base", "manifest/scores"}
    grouped = catalog.resolve("train", ["b", "a"], ["input_ids", "attention_mask"])
    assert catalog.resolve("train", ["a", "b"])["fields"] == [
        "input_ids",
        "attention_mask",
        "score",
    ]
    assert grouped["field_groups"] == [
        {
            "fields": ["input_ids", "attention_mask"],
            "locations": [("manifest/base", 1), ("manifest/base", 0)],
        }
    ]


def test_catalog_drops_replaced_and_removed_locations() -> None:
    catalog = DataProtoCatalog()
    base = handle("base", ["value"])
    replacement = handle("replacement", ["value"])
    catalog.update("train", ["a", "b"], handle=base)

    catalog.update("train", ["a"], handle={**replacement, "batch_size": 1})
    catalog.update("train", ["b", "a"], handle=base)
    plan = catalog.resolve("train", ["a", "b"], ["value"])
    assert plan["field_groups"] == [
        {
            "fields": ["value"],
            "locations": [("manifest/base", 1), ("manifest/base", 0)],
        }
    ]
    catalog.remove("train", ["b"])
    catalog.remove("train", ["a"])
    assert catalog.list() == {}


def test_catalog_rejects_missing_or_incomplete_reads_without_mutation() -> None:
    catalog = DataProtoCatalog()
    catalog.update("train", ["a", "b"], tags=[{}, {}])
    catalog.update("train", ["a"], handle=handle("one", ["value"], batch_size=1))

    with pytest.raises(ValueError, match="were not found"):
        catalog.resolve("train", ["missing"])
    with pytest.raises(ValueError, match="not ready"):
        catalog.resolve("train", ["a", "b"], ["value"])
    with pytest.raises(ValueError, match="do not contain any fields"):
        catalog.resolve("train", ["b"])
    with pytest.raises(ValueError, match="same length"):
        catalog.update("train", ["a", "b"], tags=[{}])
    with pytest.raises(ValueError, match="sequence"):
        catalog.resolve("train", "a")
    with pytest.raises(ValueError, match="sequence"):
        catalog.resolve("train", ["a"], "value")
    invalid_handle = handle("invalid", ["value"], batch_size=1)
    invalid_handle["version"] = 999
    with pytest.raises(ValueError, match="unsupported.*version"):
        catalog.update("invalid", ["a"], handle=invalid_handle)
    multi_stage = handle("multi", ["value"], batch_size=1)
    multi_stage["stage_refs"]["extra"] = {"manifest_key": "manifest/extra"}
    with pytest.raises(ValueError, match="exactly one.*stage"):
        catalog.update("invalid", ["a"], handle=multi_stage)

    assert catalog.list("train") == {"train": {"a": {}, "b": {}}}
    assert "invalid" not in catalog.list()


def test_catalog_only_retires_managed_fragments() -> None:
    catalog = DataProtoCatalog()
    external = handle("external", ["value"], batch_size=1)
    catalog.update("train", ["a"], handle=external)
    assert catalog.remove("train", ["a"])["retired_handles"] == []

    managed = handle("managed", ["value"], batch_size=1)
    catalog.publish("managed-op", "train", ["a"], handle=managed)
    assert catalog.publish("managed-op", "other", ["x"], handle=external)["fields"] == [
        "value"
    ]
    assert "other" not in catalog.list()
    catalog.ack_publication("managed-op")
    retired = catalog.remove("train", ["a"])["retired_handles"]
    assert len(retired) == 1
    assert retired[0]["stage_refs"] == managed["stage_refs"]
    mismatched = copy.deepcopy(retired[0])
    mismatched["field_index"]["value"]["member"] = "batch.other"
    with pytest.raises(ValueError, match="fragment mismatch"):
        catalog.ack_retired([mismatched])
    assert catalog.ack_retired(retired)["retired_handles"] == []


def test_catalog_drain_is_terminal() -> None:
    catalog = DataProtoCatalog()
    managed = handle("managed", ["value"], batch_size=1)
    catalog.publish("pending-op", "train", ["a"], handle=managed)

    retired = catalog.drain()["retired_handles"]
    with pytest.raises(RuntimeError, match="catalog is drained"):
        catalog.publish("pending-op", "train", ["a"], handle=managed)
    assert catalog.ack_retired(retired)["retired_handles"] == []


def test_catalog_append_replaces_managed_handle_ownership() -> None:
    catalog = DataProtoCatalog()
    base = handle("base", ["input", "value"], batch_size=1)
    base["storage_group_id"] = "structured-test"
    base["partition"] = "train"
    catalog.publish("put-op", "train", ["a"], handle=base)
    catalog.ack_publication("put-op")
    catalog.update(
        "train", ["a"], handle=handle("replacement", ["value"], batch_size=1)
    )

    appended = copy.deepcopy(base)
    appended["stage_refs"]["value"] = {"manifest_key": "manifest/value"}
    appended["field_index"]["score"] = {
        "stage": "value",
        "member": "batch.score",
        "section": "batch",
    }
    assert (
        "fragment identity"
        in catalog.publish_append(
            "wrong-partition",
            "manifest/base",
            "other",
            ["a"],
            previous_handle=base,
            handle=appended,
        )["append_rejected"]
    )
    catalog.publish_append(
        "append-op",
        "manifest/base",
        "train",
        ["a"],
        previous_handle=base,
        handle=appended,
    )
    assert catalog.publish_append(
        "append-op",
        "manifest/base",
        "train",
        ["a"],
        previous_handle=base,
        handle=appended,
    )["fields"] == ["input", "value", "score"]
    assert (
        "stale"
        in catalog.publish_append(
            "stale-op",
            "manifest/base",
            "train",
            ["a"],
            previous_handle=base,
            handle=appended,
        )["append_rejected"]
    )

    plan = catalog.resolve("train", ["a"])
    assert plan["fields"] == ["input", "value", "score"]
    assert set(plan["handles"]) == {"manifest/base", "manifest/replacement"}
    assert plan["handles"]["manifest/base"]["stage_refs"] == appended["stage_refs"]
    assert plan["field_groups"][1]["locations"] == [("manifest/replacement", 0)]
    retired = catalog.remove("train", ["a"])["retired_handles"]
    assert retired == [plan["handles"]["manifest/base"]]
    assert catalog.ack_retired(retired)["retired_handles"] == []


class FakeTransfer:
    def __init__(self) -> None:
        self.count = self.release_attempts = 0
        self.cleaned = []

    def put(self, data, **_kwargs):
        self.count += 1
        return import_dataproto_ref(
            handle(f"put-{self.count}", list(data.fields), len(data))
        )

    def cleanup_dataproto(self, ref) -> None:
        self.cleaned.append(next(iter(ref["stage_refs"].values()))["manifest_key"])

    def release_result(self, _result) -> None:
        self.release_attempts += 1
        if self.release_attempts == 1:
            raise RuntimeError("release failed")


def test_catalog_transfer_lifecycle_and_failure_retries() -> None:
    catalog, transfer = DataProtoCatalog(), FakeTransfer()

    def call(method, *args, **kwargs):
        return getattr(catalog, method)(*args, **kwargs)

    client = DataProtoCatalogTransfer(transfer, call)
    data = type("FakeData", (list,), {"fields": ("value",)})([None])
    client.put(data, partition="train", keys=["a"])
    first_read = client.resolve("train", ["a"])
    second_read = client.resolve("train", ["a"])
    client.remove("train", ["a"])
    client.release_read(first_read["read_token"])
    assert transfer.cleaned == []
    with pytest.raises(RuntimeError, match="active read"):
        client.drain()
    client.release_read(second_read["read_token"])
    assert transfer.cleaned == ["manifest/put-1"]

    output = SimpleNamespace()
    result = SimpleNamespace(batch={}, non_tensor_batch={}, meta_info={})
    client.attach_results(output, [result])
    client.release_result(output)
    assert transfer.release_attempts == 2

    publication_failure = "before_commit"
    lost_responses = 0

    def fail_update(method, *args, **kwargs):
        nonlocal publication_failure, lost_responses
        if method != "publish":
            return call(method, *args, **kwargs)
        if publication_failure == "before_commit":
            raise RuntimeError("publication failed")
        result = call(method, *args, **kwargs)
        if publication_failure == "after_commit" and lost_responses:
            lost_responses -= 1
            raise RuntimeError("response lost after commit")
        return result

    client._catalog_call = fail_update
    with pytest.raises(RuntimeError, match="publication failed"):
        client.put(data, partition="train", keys=["b"])
    assert transfer.cleaned == ["manifest/put-1"]

    publication_failure = None
    client.close()
    assert catalog.resolve("train", ["b"])["fields"] == ["value"]
    client.remove("train", ["b"])
    assert transfer.cleaned[-1] == "manifest/put-2"

    publication_failure = "after_commit"
    lost_responses = 2
    with pytest.raises(RuntimeError, match="response lost"):
        client.put(data, partition="train", keys=["c"])
    assert catalog.resolve("train", ["c"])["fields"] == ["value"]
    assert transfer.cleaned[-1] == "manifest/put-2"

    publication_failure = None
    client.close()
    client.remove("train", ["c"])
    assert transfer.cleaned[-1] == "manifest/put-3"


def test_catalog_transfer_drain_resolves_pending_publications() -> None:
    catalog, transfer = DataProtoCatalog(), FakeTransfer()
    fail_publication = True

    def call(method, *args, **kwargs):
        if method == "publish" and fail_publication:
            raise RuntimeError("publication failed")
        return getattr(catalog, method)(*args, **kwargs)

    client = DataProtoCatalogTransfer(transfer, call)
    data = type("FakeData", (list,), {"fields": ("value",)})([None])
    with pytest.raises(RuntimeError, match="publication failed"):
        client.put(data, partition="train", keys=["a"])
    with pytest.raises(RuntimeError, match="publish pending"):
        client.drain()

    fail_publication = False
    client.drain()
    assert transfer.cleaned == ["manifest/put-1"]


def test_catalog_retry_does_not_resurrect_a_superseded_fragment() -> None:
    catalog, transfer = DataProtoCatalog(), FakeTransfer()

    def call(method, *args, **kwargs):
        return getattr(catalog, method)(*args, **kwargs)

    lost_responses = 2

    def lose_publish_response(method, *args, **kwargs):
        nonlocal lost_responses
        result = call(method, *args, **kwargs)
        if method == "publish" and lost_responses:
            lost_responses -= 1
            raise RuntimeError("response lost after commit")
        return result

    data = type("FakeData", (list,), {"fields": ("value",)})([None])
    first = DataProtoCatalogTransfer(transfer, lose_publish_response)
    with pytest.raises(RuntimeError, match="response lost"):
        first.put(data, partition="train", keys=["a"])

    second = DataProtoCatalogTransfer(transfer, call)
    second.put(data, partition="train", keys=["a"])
    assert transfer.cleaned == ["manifest/put-1"]

    first.close()
    plan = catalog.resolve("train", ["a"])
    assert set(plan["handles"]) == {"manifest/put-2"}
    assert transfer.cleaned == ["manifest/put-1"]


def test_catalog_ack_retry_does_not_republish_a_superseded_fragment() -> None:
    catalog, transfer = DataProtoCatalog(), FakeTransfer()

    def call(method, *args, **kwargs):
        return getattr(catalog, method)(*args, **kwargs)

    lose_ack = True

    def lose_ack_response(method, *args, **kwargs):
        nonlocal lose_ack
        result = call(method, *args, **kwargs)
        if method == "ack_publication" and lose_ack:
            lose_ack = False
            raise RuntimeError("ack response lost")
        return result

    data = type("FakeData", (list,), {"fields": ("value",)})([None])
    first = DataProtoCatalogTransfer(transfer, lose_ack_response)
    first.put(data, partition="train", keys=["a"])

    second = DataProtoCatalogTransfer(transfer, call)
    second.put(data, partition="train", keys=["a"])
    first.close()

    plan = catalog.resolve("train", ["a"])
    assert set(plan["handles"]) == {"manifest/put-2"}
    assert transfer.cleaned == ["manifest/put-1"]
