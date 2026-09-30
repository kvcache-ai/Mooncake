from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from mooncake.weight_store.model import (
    ModelFileCacheClient,
    ModelFileManifest,
    READY,
    SCHEMA_VERSION,
)
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


class CleanupFailingStore(InterruptingStore):
    def __init__(self) -> None:
        super().__init__()
        self.remove_calls: list[str] = []

    def put(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        if "/chunks/" in key:
            self.chunk_puts += 1
            if self.chunk_puts == 4:
                raise KeyboardInterrupt()
        return FakeStore.put(self, key, value, config)

    def remove(self, key: str, force: bool = False) -> int:
        self.remove_calls.append(key)
        if len(self.remove_calls) == 1:
            return -1
        return super().remove(key, force)


class MaterializeFailingStore(FakeStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_get = False

    def get(self, key: str) -> bytes | None:
        if self.fail_get and "/chunks/" in key:
            raise RuntimeError("read failed")
        return super().get(key)


class CorruptReadStore(FakeStore):
    """Returns wrong bytes for exactly one chunk key on read."""

    def __init__(self, corrupt_key: str) -> None:
        super().__init__()
        self.corrupt_key = corrupt_key

    def get(self, key: str) -> bytes | None:
        value = super().get(key)
        if key == self.corrupt_key and value is not None:
            return b"corrupted-bytes-that-differ"
        return value


class RecordingStore(FakeStore):
    """Logs every write as (op, key, value) to prove ordering."""

    def __init__(self) -> None:
        super().__init__()
        self.writes: list[tuple[str, str, bytes]] = []

    def put(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        self.writes.append(("put", key, bytes(value)))
        return super().put(key, value, config)

    def upsert(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        self.writes.append(("upsert", key, bytes(value)))
        return super().upsert(key, value, config)


class MissingObjectRemoveStore(FakeStore):
    """Mimics mooncake native remove: -704 for an already-absent key."""

    def remove(self, key: str, force: bool = False) -> int:
        if key not in self.objects:
            return -704
        self.objects.pop(key, None)
        self.configs.pop(key, None)
        return 0


class ManifestRemoveFailingStore(FakeStore):
    """Fails remove() of the manifest key while a flag is set."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_manifest_remove = False

    def remove(self, key: str, force: bool = False) -> int:
        if self.fail_manifest_remove and key.endswith("/manifest"):
            return -1
        return super().remove(key, force)


class ToggleChunkFailStore(FakeStore):
    """Fails chunk puts while ``fail_chunks`` is set, healthy otherwise."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_chunks = True

    def put(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        if self.fail_chunks and "/chunks/" in key:
            return -1
        return super().put(key, value, config)



class WriteOnceStore(FakeStore):
    """Mimics the native mooncake store: it has NO ``upsert`` at all, and its
    ``put`` is write-once -- a second put to an existing key returns OK (0) but
    silently keeps the old bytes (Client::Put maps OBJECT_ALREADY_EXISTS ->
    success). ``remove`` works and returns -704 for an already-absent key. This
    is the store the blocker fix (_put_control delete-then-put) must survive.
    """

    def __getattribute__(self, name: str):
        # The real store exposes no ``upsert``; hide the one FakeStore defines so
        # the client is forced onto the delete-then-put path, as in production.
        if name == "upsert":
            raise AttributeError(name)
        return super().__getattribute__(name)

    def put(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        if key in self.objects:
            return 0  # OBJECT_ALREADY_EXISTS -> silent success; bytes unchanged
        self.objects[key] = bytes(value)
        self.configs[key] = config
        return 0

    def remove(self, key: str, force: bool = False) -> int:
        if key not in self.objects:
            return -704
        self.objects.pop(key, None)
        self.configs.pop(key, None)
        return 0


class WriteOnceManifestRemoveFailingStore(WriteOnceStore):
    """A write-once store whose manifest ``remove`` fails while a flag is set,
    to exercise a delete crash between de-index and manifest removal."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_manifest_remove = False

    def remove(self, key: str, force: bool = False) -> int:
        if self.fail_manifest_remove and key.endswith("/manifest"):
            return -1
        return super().remove(key, force)


class SurvivorChunkStore(FakeStore):
    """Interrupts the 2nd chunk put, then fails the cleanup ``remove`` of the
    first (already-stored) chunk exactly once so it survives in the store; any
    later remove (e.g. from delete_model) succeeds."""

    def __init__(self) -> None:
        super().__init__()
        self.chunk_puts = 0
        self.cleanup_remove_failed = False

    def put(self, key: str, value: bytes, config: FakeReplicateConfig) -> int:
        if "/chunks/" in key:
            self.chunk_puts += 1
            if self.chunk_puts == 2:
                raise KeyboardInterrupt()
        return super().put(key, value, config)

    def remove(self, key: str, force: bool = False) -> int:
        if "/chunks/" in key and not self.cleanup_remove_failed:
            self.cleanup_remove_failed = True
            return -1
        return super().remove(key, force)


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
        for valid in ("demo-main", "Qwen3_32B", "model.v2-alpha", "a..b", "a" * 255):
            with self.subTest(valid=valid):
                validate_checkpoint_id(valid)

        for invalid in (
            "",
            "a//b",
            "/a",
            "a/b/",
            "a/",
            ".hidden",
            "a b",
            "a/b",
            "a?b",
            ".",
            "..",
            "a" * 256,
        ):
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

    def test_import_cleanup_attempts_all_chunks_after_remove_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = CleanupFailingStore()
            client = ModelFileCacheClient(store, file_chunk_size=4)

            with self.assertRaises(KeyboardInterrupt):
                client.import_model(
                    checkpoint_id="demo-main",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )

            self.assertGreaterEqual(len(store.remove_calls), 2)
            self.assertTrue(
                any("/chunks/00000000" in key for key in store.remove_calls)
            )
            self.assertTrue(
                any("/chunks/00000001" in key for key in store.remove_calls)
            )

    def test_materialize_failure_preserves_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            output = Path(tmp) / "out" / "model.safetensors"
            source.mkdir()
            write_demo_model(source)
            store = MaterializeFailingStore()
            client = ModelFileCacheClient(store, file_chunk_size=4)
            client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )
            output.parent.mkdir(parents=True)
            output.write_bytes(b"original")
            store.fail_get = True

            with self.assertRaises(RuntimeError):
                client.materialize_file(
                    "demo-main", "model-00001-of-00002.safetensors", str(output)
                )

            self.assertEqual(output.read_bytes(), b"original")
            self.assertEqual(list(output.parent.glob(f".{output.name}.*.tmp")), [])

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

    def test_import_corrupt_chunk_is_never_published(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            corrupt_key = model_file_chunk_key("demo-main", "config.json", 0)
            store = CorruptReadStore(corrupt_key)
            client = ModelFileCacheClient(store)

            with self.assertRaises(ValueError):
                client.import_model(
                    checkpoint_id="demo-main",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )

            self.assertEqual(client.list_models(), [])
            self.assertEqual(client.inspect_model("demo-main").status, "FAILED")

    def test_import_writes_index_only_after_ready_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = RecordingStore()
            client = ModelFileCacheClient(store)
            client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

        manifest_key = model_manifest_key("demo-main")
        index_key = model_index_key()

        manifest_statuses = [
            json.loads(value.decode("utf-8"))["status"]
            for (_op, key, value) in store.writes
            if key == manifest_key
        ]
        # The manifest is written exactly once, and only as READY:
        # IMPORTING is never persisted (blocker/F1 redesign), so a
        # write-once store cannot strand it at IMPORTING.
        self.assertEqual(manifest_statuses, [READY])

        ready_pos = next(
            i
            for i, (_op, key, value) in enumerate(store.writes)
            if key == manifest_key
            and json.loads(value.decode("utf-8"))["status"] == READY
        )
        index_positions = [
            i for i, (_op, key, _v) in enumerate(store.writes) if key == index_key
        ]
        self.assertTrue(index_positions)
        self.assertTrue(all(pos > ready_pos for pos in index_positions))

    def test_delete_is_idempotent_with_missing_object_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = MissingObjectRemoveStore()
            client = ModelFileCacheClient(store)
            manifest = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

            # Simulate a chunk a prior partial delete already removed, so the
            # next delete hits the real -704 "already gone" path in _remove.
            first_chunk = manifest.files[0].chunks[0]
            store.objects.pop(first_chunk, None)

            client.delete_model("demo-main")
            client.delete_model("demo-main")

            self.assertEqual(client.list_models(), [])
            self.assertNotIn(model_manifest_key("demo-main"), store.objects)
            for record in manifest.files:
                for chunk in record.chunks:
                    self.assertNotIn(chunk, store.objects)

    def test_delete_crash_between_deindex_and_manifest_then_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = ManifestRemoveFailingStore()
            client = ModelFileCacheClient(store)
            manifest = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

            store.fail_manifest_remove = True
            with self.assertRaises(RuntimeError):
                client.delete_model("demo-main")

            for record in manifest.files:
                for chunk in record.chunks:
                    self.assertNotIn(chunk, store.objects)
            self.assertNotIn("demo-main", client.list_models())

            # After a crash between de-index and manifest removal the
            # manifest STILL EXISTS and is readable with its model_id
            # intact. DELETING is no longer persisted, so it stays at its
            # prior status (READY); the id is already de-indexed (asserted
            # above) and a retry finishes teardown.
            stranded = client.inspect_model("demo-main")
            self.assertEqual(stranded.status, READY)
            self.assertEqual(stranded.model_id, "demo/model")
            self.assertIn(model_manifest_key("demo-main"), store.objects)

            store.fail_manifest_remove = False
            client.delete_model("demo-main")

            self.assertNotIn(model_manifest_key("demo-main"), store.objects)
            self.assertEqual(client.list_models(), [])

    def test_delete_of_never_imported_id_is_a_noop(self) -> None:
        store = FakeStore()
        client = ModelFileCacheClient(store)

        client.delete_model("ghost")

        self.assertEqual(client.list_models(), [])

    def test_delete_of_corrupt_manifest_removes_dangling_key(self) -> None:
        store = FakeStore()
        manifest_key = model_manifest_key("demo-main")
        store.objects[manifest_key] = b"{not json"
        client = ModelFileCacheClient(store)

        client.delete_model("demo-main")

        self.assertNotIn(manifest_key, store.objects)

    def test_import_reclaims_failed_checkpoint_and_reimports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = ToggleChunkFailStore()
            client = ModelFileCacheClient(store)

            with self.assertRaises(RuntimeError):
                client.import_model(
                    checkpoint_id="demo-main",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )
            self.assertEqual(client.inspect_model("demo-main").status, "FAILED")
            self.assertEqual(client.list_models(), [])

            store.fail_chunks = False
            manifest = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

            self.assertEqual(manifest.status, READY)
            self.assertIn("demo-main", client.list_models())

    def test_import_still_rejects_ready_checkpoint(self) -> None:
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

            with self.assertRaises(ValueError) as ctx:
                client.import_model(
                    checkpoint_id="demo-main",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )
            self.assertIn("demo-main", str(ctx.exception))

    # ------------------------------------------------------------------
    # Blocker: the native store is write-once and has no ``upsert``.
    # ------------------------------------------------------------------
    def test_two_imports_visible_on_write_once_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = WriteOnceStore()
            self.assertFalse(hasattr(store, "upsert"))
            client = ModelFileCacheClient(store, file_chunk_size=4)
            client.import_model(
                checkpoint_id="ckpt-a",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )
            client.import_model(
                checkpoint_id="ckpt-b",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

        # With a naive write-once put the 2nd index write is silently dropped
        # and ckpt-b vanishes; delete-then-put in _put_control keeps both.
        self.assertEqual(client.list_models(), ["ckpt-a", "ckpt-b"])

    def test_import_ready_on_write_once_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = WriteOnceStore()
            client = ModelFileCacheClient(store, file_chunk_size=4)
            manifest = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

        self.assertEqual(manifest.status, READY)
        # The persisted manifest is READY, not stranded at IMPORTING (which a
        # write-once put would do if IMPORTING were persisted then overwritten).
        self.assertEqual(client.inspect_model("demo-main").status, READY)
        self.assertEqual(client.verify_model("demo-main").status, READY)
        self.assertIn("demo-main", client.list_models())

    def test_delete_then_reimport_same_id_on_write_once_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = WriteOnceStore()
            client = ModelFileCacheClient(store, file_chunk_size=4)
            first = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

            client.delete_model("demo-main")

            # Delete leaves nothing behind: manifest gone, all chunks gone,
            # nothing indexed.
            self.assertEqual(client.list_models(), [])
            self.assertNotIn(model_manifest_key("demo-main"), store.objects)
            for record in first.files:
                for chunk in record.chunks:
                    self.assertNotIn(chunk, store.objects)

            # The manifest (and index) key is reusable after remove: a re-import
            # of the same id succeeds and ends READY.
            second = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

        self.assertEqual(second.status, READY)
        self.assertEqual(client.inspect_model("demo-main").status, READY)
        self.assertIn("demo-main", client.list_models())

    def test_delete_deindexes_before_removing_manifest_write_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = WriteOnceManifestRemoveFailingStore()
            client = ModelFileCacheClient(store, file_chunk_size=4)
            manifest = client.import_model(
                checkpoint_id="demo-main",
                model_id="demo/model",
                revision="main",
                source_uri=str(source),
            )

            store.fail_manifest_remove = True
            with self.assertRaises(RuntimeError):
                client.delete_model("demo-main")

            # De-indexed and chunks gone, but the manifest survives the crash and
            # stays parseable with an intact model_id and status READY (never
            # DELETING) -- so the id can never be stranded (Finding 4).
            self.assertNotIn("demo-main", client.list_models())
            for record in manifest.files:
                for chunk in record.chunks:
                    self.assertNotIn(chunk, store.objects)
            stranded = client.inspect_model("demo-main")
            self.assertEqual(stranded.status, READY)
            self.assertEqual(stranded.model_id, "demo/model")

            store.fail_manifest_remove = False
            client.delete_model("demo-main")

            self.assertNotIn(model_manifest_key("demo-main"), store.objects)
            self.assertEqual(client.list_models(), [])

    # ------------------------------------------------------------------
    # F2: the authoritative chunk_size is persisted (and legacy-safe).
    # ------------------------------------------------------------------
    def test_manifest_records_carry_chunk_size(self) -> None:
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

        self.assertEqual(manifest.schema_version, SCHEMA_VERSION)
        self.assertEqual(SCHEMA_VERSION, 2)
        self.assertTrue(manifest.files)
        for record in manifest.files:
            self.assertEqual(record.chunk_size, 4)

        # A JSON round-trip preserves chunk_size.
        round_tripped = ModelFileManifest.from_json_bytes(manifest.to_json_bytes())
        for record in round_tripped.files:
            self.assertEqual(record.chunk_size, 4)

        # A legacy manifest dict WITHOUT chunk_size parses with the default 0.
        legacy = json.loads(manifest.to_json_bytes().decode("utf-8"))
        for item in legacy["files"]:
            item.pop("chunk_size", None)
        legacy_manifest = ModelFileManifest.from_json_bytes(
            json.dumps(legacy).encode("utf-8")
        )
        self.assertTrue(legacy_manifest.files)
        for record in legacy_manifest.files:
            self.assertEqual(record.chunk_size, 0)

    # ------------------------------------------------------------------
    # F3: a chunk whose cleanup remove fails is recorded and later swept.
    # ------------------------------------------------------------------
    def test_failed_import_preserves_unremovable_chunk_for_later_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp)
            write_demo_model(source)
            store = SurvivorChunkStore()
            client = ModelFileCacheClient(store, file_chunk_size=4)

            with self.assertRaises(KeyboardInterrupt):
                client.import_model(
                    checkpoint_id="demo-main",
                    model_id="demo/model",
                    revision="main",
                    source_uri=str(source),
                )

            # The immediate cleanup remove of the first chunk failed, so it
            # survives in the store.
            survivor = model_file_chunk_key("demo-main", "config.json", 0)
            self.assertIn(survivor, store.objects)

            # The FAILED manifest records the survivor so it is not orphaned.
            failed = client.inspect_model("demo-main")
            self.assertEqual(failed.status, "FAILED")
            recorded = [chunk for record in failed.files for chunk in record.chunks]
            self.assertIn(survivor, recorded)

            # delete_model then sweeps it: no orphan chunk remains.
            client.delete_model("demo-main")
            self.assertNotIn(survivor, store.objects)
            self.assertNotIn(model_manifest_key("demo-main"), store.objects)


if __name__ == "__main__":
    unittest.main()
