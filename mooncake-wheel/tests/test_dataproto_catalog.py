from __future__ import annotations

import copy
import threading
import time
from types import SimpleNamespace

import numpy as np
import pytest

from mooncake.dataproto_catalog import DataProtoCatalog, DataProtoCatalogTransfer
from mooncake.structured_object_store import import_dataproto_ref


def handle(
    name: str, fields: list[str], batch_size: int = 2, partition: str = "default"
) -> dict:
    return {
        "type": "mooncake_dataproto_ref",
        "version": 1,
        "kind": "bundle_stages",
        "batch_size": batch_size,
        "partition": partition,
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


class ShardTransfer:
    """Serve row ranges from in-memory fragments and record each read."""

    def __init__(self, fragments: dict[str, dict]) -> None:
        self.fragments = fragments
        self.calls: list[tuple[str, int, int, tuple[str, ...]]] = []
        self.cleaned: list[str] = []

    def get_dataproto(self, ref, *, fields, rows):
        result = {"batch": {}, "non_tensor_batch": {}, "meta_info": {}}
        for name in fields:
            stage = ref["field_index"][name]["stage"]
            fragment_id = ref["stage_refs"][stage]["manifest_key"]
            self.calls.append((fragment_id, rows.start, rows.stop, (name,)))
            data = self.fragments[fragment_id]
            if name in data["batch"]:
                result["batch"][name] = data["batch"][name][rows]
            else:
                result["non_tensor_batch"][name] = data["non_tensor_batch"][name][rows]
        return result

    def cleanup_dataproto(self, ref):
        self.cleaned.append(next(iter(ref["stage_refs"].values()))["manifest_key"])


def catalog_client(transfer):
    catalog = DataProtoCatalog()

    def call(method, *args, **kwargs):
        return getattr(catalog, method)(*args, **kwargs)

    return catalog, DataProtoCatalogTransfer(transfer, call)


def sharded_fragments(values: np.ndarray, shards: int) -> dict[str, dict]:
    rows = len(values) // shards
    return {
        f"manifest/s{i}": {
            "batch": {"score": values[i * rows : (i + 1) * rows].copy()},
            "non_tensor_batch": {},
        }
        for i in range(shards)
    }


def test_catalog_transfer_registers_shards_and_reads_in_key_order() -> None:
    values = np.arange(6, dtype=np.float32).reshape(6, 1)
    transfer = ShardTransfer(sharded_fragments(values, 3))
    catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(6)]
    for i in range(3):
        client.register("train", keys[i * 2 : (i + 1) * 2], handle(f"s{i}", ["score"]))

    result = client.read("train", keys, ["score"])

    np.testing.assert_array_equal(result["batch"]["score"], values)
    assert transfer.calls == [
        ("manifest/s0", 0, 2, ("score",)),
        ("manifest/s1", 0, 2, ("score",)),
        ("manifest/s2", 0, 2, ("score",)),
    ]
    assert catalog._read_pins == {}


def test_catalog_transfer_read_honors_reordered_keys() -> None:
    values = np.arange(4, dtype=np.float32).reshape(4, 1)
    transfer = ShardTransfer(sharded_fragments(values, 2))
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys[:2], handle("s0", ["score"]))
    client.register("train", keys[2:], handle("s1", ["score"]))

    result = client.read("train", keys[::-1], ["score"])

    np.testing.assert_array_equal(result["batch"]["score"], values[::-1])
    assert all(call[1] == call[2] - 1 for call in transfer.calls)


def test_catalog_transfer_read_merges_non_tensor_fields() -> None:
    values = np.array(["a", "bb", "ccc", "dddd"], dtype=object).reshape(4, 1)
    fragments = {
        f"manifest/t{i}": {
            "batch": {},
            "non_tensor_batch": {"text": values[i * 2 : (i + 1) * 2].copy()},
        }
        for i in range(2)
    }
    transfer = ShardTransfer(fragments)
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys[:2], handle("t0", ["text"]))
    client.register("train", keys[2:], handle("t1", ["text"]))

    result = client.read("train", keys, ["text"])

    assert list(result["non_tensor_batch"]["text"][:, 0]) == ["a", "bb", "ccc", "dddd"]


def test_catalog_transfer_read_rejects_incomplete_fields() -> None:
    transfer = ShardTransfer(sharded_fragments(np.zeros((4, 1), np.float32), 2))
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys[:2], handle("s0", ["score"]))
    client.register("train", keys[2:], handle("s1", ["other"]))

    with pytest.raises(ValueError, match="fields are not ready"):
        client.read("train", keys, ["score"])


def test_catalog_transfer_read_releases_pin_on_error() -> None:
    class FailingTransfer:
        def get_dataproto(self, ref, *, fields, rows):
            raise RuntimeError("read failed")

    catalog, client = catalog_client(FailingTransfer())
    client.register("train", ["a", "b"], handle("s0", ["score"]))

    with pytest.raises(RuntimeError, match="read failed"):
        client.read("train", ["a", "b"], ["score"])

    assert catalog._read_pins == {}
    assert catalog._fragment_readers == {}


class BatchData:
    def __init__(self, **fields):
        self.fields = fields

    def __len__(self):
        return len(next(iter(self.fields.values())))


class AppendTransfer(ShardTransfer):
    def append_dataproto_fields(self, handle, data, *, stage):
        field, value = next(iter(data.fields.items()))
        appended = copy.deepcopy(handle)
        appended["stage_refs"][stage] = {"manifest_key": f"manifest/{stage}"}
        appended["field_index"][field] = {
            "stage": stage,
            "member": f"batch.{field}",
            "section": "batch",
        }
        self.fragments[f"manifest/{stage}"] = {
            "batch": {field: value},
            "non_tensor_batch": {},
        }
        return import_dataproto_ref(appended)

    def cleanup_dataproto_append(self, previous_handle, handle):
        return None


def test_catalog_transfer_appends_to_registered_fragment() -> None:
    values = np.arange(4, dtype=np.float32).reshape(4, 1)
    transfer = AppendTransfer(sharded_fragments(values, 1))
    _catalog, client = catalog_client(transfer)
    keys = ["k0", "k1", "k2", "k3"]
    client.register(
        "train", keys, handle("s0", ["score"], batch_size=4, partition="train")
    )
    plan = client.resolve("train", keys, ["score"])
    fragment_id, fragment_handle = next(iter(plan["handles"].items()))
    advantage = np.full((4, 1), 7.0, dtype=np.float32)

    client.append(
        fragment_id,
        fragment_handle,
        BatchData(advantage=advantage),
        partition="train",
        keys=keys,
        stage="advantage",
    )

    result = client.read("train", keys, ["score", "advantage"])
    np.testing.assert_array_equal(result["batch"]["score"], values)
    np.testing.assert_array_equal(result["batch"]["advantage"], advantage)


def test_catalog_transfer_rejects_append_for_unmanaged_fragment() -> None:
    values = np.arange(4, dtype=np.float32).reshape(4, 1)
    transfer = AppendTransfer(sharded_fragments(values, 1))
    _catalog, client = catalog_client(transfer)
    keys = ["k0", "k1", "k2", "k3"]
    client.register(
        "train",
        keys,
        handle("s0", ["score"], batch_size=4, partition="train"),
        managed=False,
    )
    plan = client.resolve("train", keys, ["score"])
    fragment_id, fragment_handle = next(iter(plan["handles"].items()))

    with pytest.raises(ValueError, match="stale DataProto append handle"):
        client.append(
            fragment_id,
            fragment_handle,
            BatchData(advantage=np.zeros((4, 1), np.float32)),
            partition="train",
            keys=keys,
            stage="advantage",
        )


def test_catalog_transfer_serializes_concurrent_catalog_calls() -> None:
    active = 0
    overlap: list[int] = []
    guard = threading.Lock()

    class SlowCatalog(DataProtoCatalog):
        def update(self, *args, **kwargs):
            nonlocal active
            with guard:
                active += 1
                if active > 1:
                    overlap.append(active)
            time.sleep(0.001)
            try:
                return super().update(*args, **kwargs)
            finally:
                with guard:
                    active -= 1

    catalog = SlowCatalog()

    def call(method, *args, **kwargs):
        return getattr(catalog, method)(*args, **kwargs)

    client = DataProtoCatalogTransfer(ShardTransfer({}), call)
    keys = [f"k{i}" for i in range(4)]
    threads = [
        threading.Thread(
            target=client.register,
            args=("train", keys, handle("s0", ["score"], batch_size=4)),
            kwargs={"managed": False},
        )
        for _ in range(8)
    ]

    [thread.start() for thread in threads]
    [thread.join() for thread in threads]

    assert overlap == []
    assert catalog._fragment_refcounts == {"manifest/s0": 4}


def test_catalog_transfer_rejects_replayed_append_handle() -> None:
    values = np.arange(4, dtype=np.float32).reshape(4, 1)
    transfer = AppendTransfer(sharded_fragments(values, 1))
    _catalog, client = catalog_client(transfer)
    keys = ["k0", "k1", "k2", "k3"]
    client.register(
        "train", keys, handle("s0", ["score"], batch_size=4, partition="train")
    )
    plan = client.resolve("train", keys, ["score"])
    fragment_id, fragment_handle = next(iter(plan["handles"].items()))
    client.release_read(plan["read_token"])
    client.append(
        fragment_id,
        fragment_handle,
        BatchData(advantage=np.zeros((4, 1), np.float32)),
        partition="train",
        keys=keys,
        stage="advantage",
    )

    with pytest.raises(ValueError, match="stale DataProto append handle"):
        client.append(
            fragment_id,
            fragment_handle,
            BatchData(credit=np.ones((4, 1), np.float32)),
            partition="train",
            keys=keys,
            stage="credit",
        )


def test_catalog_transfer_read_builds_data_class() -> None:
    class Result:
        @classmethod
        def from_dict(cls, batch, non_tensor_batch, meta_info=None):
            return cls(batch, non_tensor_batch, meta_info)

        def __init__(self, batch, non_tensor_batch, meta_info) -> None:
            self.batch = batch
            self.non_tensor_batch = non_tensor_batch
            self.meta_info = meta_info

    values = np.arange(4, dtype=np.float32).reshape(4, 1)
    transfer = ShardTransfer(sharded_fragments(values, 2))
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys[:2], handle("s0", ["score"], batch_size=2))
    client.register("train", keys[2:], handle("s1", ["score"], batch_size=2))

    result = client.read("train", keys, ["score"], data_cls=Result)

    assert isinstance(result, Result)
    np.testing.assert_array_equal(result.batch["score"], values)
    assert result.meta_info == {}


def test_catalog_transfer_read_checks_row_counts() -> None:
    class TruncatingTransfer(ShardTransfer):
        def get_dataproto(self, ref, *, fields, rows):
            result = super().get_dataproto(ref, fields=fields, rows=rows)
            return {
                "batch": {name: value[:-1] for name, value in result["batch"].items()},
                "non_tensor_batch": result["non_tensor_batch"],
                "meta_info": {},
            }

    transfer = TruncatingTransfer(sharded_fragments(np.zeros((4, 1), np.float32), 1))
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys, handle("s0", ["score"], batch_size=4))

    with pytest.raises(ValueError, match="read 3 rows, expected 4"):
        client.read("train", keys, ["score"])


def test_catalog_transfer_rejects_overlapping_register() -> None:
    transfer = ShardTransfer(sharded_fragments(np.zeros((4, 1), np.float32), 1))
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys, handle("s0", ["score"], batch_size=4))
    client.register("train", keys, handle("s0", ["score"], batch_size=4))

    with pytest.raises(ValueError, match="already carry these fields"):
        client.register("train", keys, handle("s1", ["score"], batch_size=4))
    with pytest.raises(ValueError, match="already carry these fields"):
        client.register(
            "train",
            keys,
            handle("s2", ["score"], batch_size=4),
            managed=False,
        )
    assert client._pending_publications == []


def test_catalog_transfer_register_overwrite_replaces_field() -> None:
    values = np.zeros((4, 1), np.float32)
    transfer = ShardTransfer(sharded_fragments(values, 1))
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys, handle("s0", ["score"], batch_size=4))

    client.register(
        "train", keys, handle("s1", ["score"], batch_size=4), overwrite=True
    )

    plan = client.resolve("train", keys, ["score"])
    assert set(plan["handles"]) == {"manifest/s1"}
    assert transfer.cleaned == ["manifest/s0"]


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


def test_register_current_handle_keeps_appended_fragment() -> None:
    values = np.arange(4, dtype=np.float32).reshape(4, 1)
    transfer = ShardTransfer(
        {
            "manifest/base-v2": {
                "batch": {"value": values},
                "non_tensor_batch": {},
            }
        }
    )
    catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    base = handle("base", ["value"], batch_size=4, partition="train")
    client.register("train", keys, base)
    appended = copy.deepcopy(base)
    appended["stage_refs"]["rollout"]["manifest_key"] = "manifest/base-v2"
    catalog.publish_append(
        "append-op",
        "manifest/base",
        "train",
        keys,
        previous_handle=base,
        handle=appended,
    )
    catalog.ack_publication("append-op")
    current = catalog.resolve("train", keys)["handles"]["manifest/base"]
    for overwrite in (False, True):
        client.register("train", keys, current, overwrite=overwrite)
    assert transfer.cleaned == []
    result = client.read("train", keys, ["value"])
    np.testing.assert_array_equal(result["batch"]["value"], values)
    assert set(catalog.resolve("train", keys)["handles"]) == {"manifest/base"}


class _OneSlotTransfer(ShardTransfer):
    def __init__(self, fragments: dict[str, dict]) -> None:
        super().__init__(fragments)
        self.inflight = 0
        self.max_inflight = 0

    def get_dataproto(self, ref, *, fields, rows):
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        if self.inflight > 1:
            raise RuntimeError("buffer pool blocked")
        return super().get_dataproto(ref, fields=fields, rows=rows)

    def release_result(self, _part) -> None:
        self.inflight -= 1


def test_read_releases_each_run_before_the_next() -> None:
    values = np.arange(4, dtype=np.float32).reshape(4, 1)
    transfer = _OneSlotTransfer(sharded_fragments(values, 2))
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys[:2], handle("s0", ["score"]))
    client.register("train", keys[2:], handle("s1", ["score"]))
    result = client.read("train", keys, ["score"])
    np.testing.assert_array_equal(result["batch"]["score"], values)
    assert transfer.max_inflight == 1
    assert transfer.inflight == 0


class _AliasTransfer:
    def __init__(self) -> None:
        self.bases: list[np.ndarray] = []
        self.cleaned: list[str] = []

    def get_dataproto(self, ref, *, fields, rows):
        shard = int(ref["stage_refs"]["rollout"]["manifest_key"].rsplit("s", 1)[-1])
        base = np.arange(
            shard * 2, shard * 2 + (rows.stop - rows.start), dtype=np.int32
        )
        self.bases.append(base)
        payload = np.empty(len(base), dtype=object)
        for index in range(len(base)):
            payload[index] = base[index : index + 1]
        return {"batch": {"rows": payload}, "non_tensor_batch": {}, "meta_info": {}}

    def release_result(self, _part) -> None:
        for base in self.bases:
            base.fill(-1)

    def cleanup_dataproto(self, ref) -> None:
        self.cleaned.append(next(iter(ref["stage_refs"].values()))["manifest_key"])


def test_read_copies_ragged_rows_before_releasing_their_buffer() -> None:
    transfer = _AliasTransfer()
    _catalog, client = catalog_client(transfer)
    keys = [f"k{i}" for i in range(4)]
    client.register("train", keys[:2], handle("s0", ["rows"]))
    client.register("train", keys[2:], handle("s1", ["rows"]))
    result = client.read("train", keys, ["rows"])
    rows = [int(row[0]) for row in result["batch"]["rows"]]
    assert rows == [0, 1, 2, 3]
    assert transfer.bases
    assert all(int(base[0]) == -1 for base in transfer.bases)


def test_confirmed_overlap_does_not_stay_in_the_retry_queue() -> None:
    catalog = DataProtoCatalog()
    failures = {"publish": 2}

    def call(method, *args, **kwargs):
        if method == "publish" and failures["publish"]:
            failures["publish"] -= 1
            raise RuntimeError("connection reset")
        return getattr(catalog, method)(*args, **kwargs)

    transfer = ShardTransfer({})
    client = DataProtoCatalogTransfer(transfer, call)
    other = DataProtoCatalogTransfer(
        transfer,
        lambda method, *args, **kwargs: getattr(catalog, method)(*args, **kwargs),
    )
    queued = handle("queued", ["score"], batch_size=1)
    with pytest.raises(RuntimeError, match="connection reset"):
        client.register("train", ["a"], queued)
    assert len(client._pending_publications) == 1
    other.register("train", ["a"], handle("owner", ["score"], batch_size=1))
    client.register("train", ["b"], handle("other", ["score"], batch_size=1))
    assert client._pending_publications == []
    plan = catalog.resolve("train", ["a", "b"], ["score"])
    assert [location[0] for location in plan["field_groups"][0]["locations"]] == [
        "manifest/owner",
        "manifest/other",
    ]


def test_lost_publish_response_replays_the_same_operation() -> None:
    catalog = DataProtoCatalog()
    lost = {"publish": True}

    def call(method, *args, **kwargs):
        result = getattr(catalog, method)(*args, **kwargs)
        if method == "publish" and lost["publish"]:
            lost["publish"] = False
            raise ValueError("response decode failed")
        return result

    transfer = ShardTransfer(sharded_fragments(np.zeros((1, 1), np.float32), 1))
    client = DataProtoCatalogTransfer(transfer, call)
    client.register("train", ["a"], handle("s0", ["score"], batch_size=1))
    assert client._pending_publications == []
    assert set(catalog.resolve("train", ["a"])["handles"]) == {"manifest/s0"}


def test_failed_read_pin_release_is_retried_on_close() -> None:
    catalog = DataProtoCatalog()
    failures = {"release_read": 1}

    def call(method, *args, **kwargs):
        if method == "release_read" and failures["release_read"]:
            failures["release_read"] -= 1
            raise RuntimeError("release rpc failed")
        return getattr(catalog, method)(*args, **kwargs)

    values = np.zeros((2, 1), np.float32)

    class _PinTransfer(ShardTransfer):
        def release_result(self, _result) -> None:
            return None

    transfer = _PinTransfer(sharded_fragments(values, 1))
    client = DataProtoCatalogTransfer(transfer, call)
    keys = ["k0", "k1"]
    client.register("train", keys, handle("s0", ["score"]))
    client.read("train", keys, ["score"])
    assert catalog._read_pins
    assert client._pending_read_tokens
    client.close()
    assert catalog._read_pins == {}
    assert client._pending_read_tokens == []


def test_put_omits_legacy_allow_overlap_keyword() -> None:
    catalog = DataProtoCatalog()
    seen: list[tuple[str, dict]] = []

    def call(method, *args, **kwargs):
        seen.append((method, kwargs))
        return getattr(catalog, method)(*args, **kwargs)

    transfer = FakeTransfer()
    client = DataProtoCatalogTransfer(transfer, call)
    data = type("FakeData", (list,), {"fields": ("value",)})([None])
    client.put(data, partition="train", keys=["a"])
    publish_kwargs = [kwargs for method, kwargs in seen if method == "publish"]
    assert publish_kwargs
    assert "allow_overlap" not in publish_kwargs[0]
    client.register(
        "train",
        ["b"],
        handle("external", ["value"], batch_size=1),
        overwrite=False,
    )
    overlap_kwargs = [kwargs for method, kwargs in seen if method == "publish"][-1]
    assert overlap_kwargs["allow_overlap"] is False


def test_unmanaged_overwrite_cleans_the_replaced_managed_fragment() -> None:
    values = np.zeros((2, 1), np.float32)
    fragments = sharded_fragments(values, 1)
    fragments["manifest/external"] = {
        "batch": {"score": values.copy()},
        "non_tensor_batch": {},
    }
    transfer = ShardTransfer(fragments)
    _catalog, client = catalog_client(transfer)
    keys = ["k0", "k1"]
    client.register("train", keys, handle("s0", ["score"]))
    client.register(
        "train",
        keys,
        handle("external", ["score"]),
        managed=False,
        overwrite=True,
    )
    assert transfer.cleaned == ["manifest/s0"]
    result = client.read("train", keys, ["score"])
    np.testing.assert_array_equal(result["batch"]["score"], values)


def test_concat_field_preserves_jagged_nested_layout() -> None:
    torch = pytest.importorskip("torch")
    from mooncake.dataproto_catalog import _concat_field

    rows = [torch.arange(length) for length in (1, 3, 2, 4)]
    left = torch.nested.as_nested_tensor(rows[:2], layout=torch.jagged)
    right = torch.nested.as_nested_tensor(rows[2:], layout=torch.jagged)
    merged = _concat_field([left, right])
    assert merged.is_nested
    assert merged.layout == torch.jagged
    assert getattr(merged, "_ragged_idx", None) == getattr(left, "_ragged_idx", None)
    for actual, expected in zip(merged.unbind(), rows):
        assert torch.equal(actual, expected)
