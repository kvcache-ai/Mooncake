from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from mooncake.weight_store.model import ModelFileCacheClient, READY
from mooncake.weight_store.model_keyspace import (
    model_file_chunk_key,
    model_file_key,
    model_index_key,
    model_manifest_key,
    validate_checkpoint_id,
)


class FakeReplicateConfig:
    def __init__(self) -> None:
        self.replica_num = 0
        self.with_hard_pin = False
        self.data_type = None


class FakeObjectDataType:
    WEIGHT = "WEIGHT"
    METADATA = "METADATA"


class FakeStore:
    ReplicateConfig = FakeReplicateConfig
    ObjectDataType = FakeObjectDataType

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.configs: dict[str, FakeReplicateConfig] = {}

    def put(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        self.objects[key] = bytes(value)
        self.configs[key] = config
        return 0

    def upsert(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        self.objects[key] = bytes(value)
        self.configs[key] = config
        return 0

    def get(self, key: str) -> bytes | None:
        return self.objects.get(key)

    def remove(self, key: str, force: bool = False) -> int:
        self.objects.pop(key, None)
        self.configs.pop(key, None)
        return 0

    def is_exist(self, key: str) -> int:
        return int(key in self.objects)


class EmptyBytesMissingStore(FakeStore):
    def get(self, key: str) -> bytes | None:
        return self.objects.get(key, b"")


class FailingStore(FakeStore):
    def __init__(self, fail_on_key_part: str) -> None:
        super().__init__()
        self.fail_on_key_part = fail_on_key_part

    def put(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        if self.fail_on_key_part in key:
            return -1
        return super().put(key, value, config)


class InterruptingStore(FakeStore):
    def __init__(self) -> None:
        super().__init__()
        self.chunk_puts = 0

    def put(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        if "/chunks/" in key:
            self.chunk_puts += 1
            if self.chunk_puts == 2:
                raise KeyboardInterrupt()
        return super().put(key, value, config)


def write_demo_model(root: Path) -> None:
    (root / "config.json").write_text('{"model_type":"demo"}', encoding="utf-8")
    (root / "tokenizer.json").write_text('{"tokens":[]}', encoding="utf-8")
    (root / "model-00001-of-00002.safetensors").write_bytes(b"weights-1")
    (root / "model-00002-of-00002.safetensors").write_bytes(b"weights-2")
    index = {
        "metadata": {"total_size": 18},
        "weight_map": {
            "layer.0.weight": "model-00001-of-00002.safetensors",
            "layer.1.weight": "model-00002-of-00002.safetensors",
        },
    }
    (root / "model.safetensors.index.json").write_text(
        json.dumps(index), encoding="utf-8"
    )


class TestModelFileCacheClient(unittest.TestCase):
    def test_import_model_writes_file_manifest_weight_metadata_and_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = FakeStore()
            client = ModelFileCacheClient(store, file_chunk_size=4)

            manifest = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

        self.assertEqual(manifest.status, READY)
        self.assertEqual(
            manifest.total_size, sum(record.size for record in manifest.files)
        )
        self.assertIn(model_manifest_key("demo-main"), store.objects)
        self.assertIn("demo-main", client.list_models())
        self.assertEqual(client.inspect_model("demo-main"), manifest)

        by_path = {record.path: record for record in manifest.files}
        weight_record = by_path["model-00001-of-00002.safetensors"]
        metadata_record = by_path["config.json"]
        self.assertEqual(
            weight_record.chunks[0],
            model_file_chunk_key("demo-main", "model-00001-of-00002.safetensors", 0),
        )
        self.assertGreater(len(weight_record.chunks), 1)
        self.assertEqual(
            store.configs[weight_record.chunks[0]].data_type, FakeObjectDataType.WEIGHT
        )
        self.assertEqual(
            store.configs[metadata_record.chunks[0]].data_type,
            FakeObjectDataType.METADATA,
        )
        self.assertTrue(store.configs[weight_record.chunks[0]].with_hard_pin)
        self.assertIn(model_index_key(), store.objects)

    def test_import_model_rejects_safetensors_index_with_missing_shard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            (source / "config.json").write_text("{}", encoding="utf-8")
            (source / "model.safetensors.index.json").write_text(
                json.dumps({"weight_map": {"x": "missing.safetensors"}}),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                ModelFileCacheClient(FakeStore()).import_model(
                    checkpoint_id="bad",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )

    def test_list_models_treats_empty_bytes_missing_index_as_empty(self) -> None:
        client = ModelFileCacheClient(EmptyBytesMissingStore())

        self.assertEqual(client.list_models(), [])

    def test_inspect_model_reports_empty_manifest_as_missing(self) -> None:
        store = FakeStore()
        store.objects[model_manifest_key("demo-main")] = b""
        client = ModelFileCacheClient(store)

        with self.assertRaises(KeyError):
            client.inspect_model("demo-main")

    def test_import_model_rejects_existing_checkpoint_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            client = ModelFileCacheClient(FakeStore())
            client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

            with self.assertRaises(ValueError):
                client.import_model(
                    checkpoint_id="demo-main",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )

    def test_checkpoint_id_validation_uses_a_single_segment_whitelist(self) -> None:
        for valid in ("demo-main", "Qwen3_32B", "model.v2-alpha"):
            with self.subTest(valid=valid):
                validate_checkpoint_id(valid)

        for invalid in ("", "a//b", "/a", "a/b/", ".hidden", "a b", "a/b", "a?b"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    validate_checkpoint_id(invalid)

    def test_import_model_rejects_invalid_checkpoint_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            client = ModelFileCacheClient(FakeStore())

            with self.assertRaises(ValueError):
                client.import_model(
                    checkpoint_id="a//b",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )

    def test_verify_model_detects_missing_or_modified_file_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = FakeStore()
            client = ModelFileCacheClient(store)
            manifest = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

        verified = client.verify_model("demo-main")
        self.assertEqual(verified.status, READY)

        store.objects[manifest.files[0].chunks[0]] = b"corrupted"
        with self.assertRaises(ValueError):
            client.verify_model("demo-main")

    def test_import_failure_writes_failed_manifest_for_inspection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = FailingStore(fail_on_key_part="/chunks/")
            client = ModelFileCacheClient(store)

            with self.assertRaises(RuntimeError):
                client.import_model(
                    checkpoint_id="demo-main",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )

            failed = client.inspect_model("demo-main")
            self.assertEqual(failed.status, "FAILED")
            self.assertIn("failed to put", failed.error or "")

    def test_import_interrupt_writes_failed_manifest_and_removes_partial_chunks(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = InterruptingStore()
            client = ModelFileCacheClient(store, file_chunk_size=4)

            with self.assertRaises(KeyboardInterrupt):
                client.import_model(
                    checkpoint_id="demo-main",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )

            failed = client.inspect_model("demo-main")
            self.assertEqual(failed.status, "FAILED")
            self.assertIn("KeyboardInterrupt", failed.error or "")
            self.assertFalse(
                any(key.endswith("/chunks/00000000") for key in store.objects)
            )

    def test_materialize_and_delete_model_follow_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            output = Path(tmp) / "out" / "config.json"
            source.mkdir()
            write_demo_model(source)
            store = FakeStore()
            client = ModelFileCacheClient(store)
            manifest = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

            client.materialize_file("demo-main", "config.json", str(output))
            self.assertEqual(
                output.read_text(encoding="utf-8"), '{"model_type":"demo"}'
            )

            client.delete_model("demo-main")
            for record in manifest.files:
                for chunk in record.chunks:
                    self.assertNotIn(chunk, store.objects)
            self.assertNotIn(model_manifest_key("demo-main"), store.objects)
            self.assertNotIn("demo-main", client.list_models())

    def test_model_file_keys_are_stable_and_sanitize_paths(self) -> None:
        self.assertEqual(
            model_manifest_key("demo-main"),
            "weight/models/demo-main/manifest",
        )
        self.assertTrue(
            model_file_key("demo-main", "nested/model.safetensors").startswith(
                "weight/models/demo-main/files/"
            )
        )


if __name__ == "__main__":
    unittest.main()
