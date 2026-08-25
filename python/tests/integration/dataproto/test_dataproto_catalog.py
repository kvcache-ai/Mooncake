from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

from mooncake.dataproto_catalog import DataProtoCatalog


def test_catalog_source_import_keeps_structured_object_import_lazy() -> None:
    repository_root = Path(__file__).resolve().parents[4]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        (
            str(repository_root / "mooncake-wheel"),
            str(repository_root / "python"),
        )
    )
    environment["PYTHONNOUSERSITE"] = "1"

    subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; import mooncake.dataproto_catalog as catalog; "
                "assert catalog.DataProtoCatalog is not None; "
                "assert 'mooncake.structured_object_store' not in sys.modules"
            ),
        ],
        cwd=repository_root,
        env=environment,
        check=True,
    )


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
