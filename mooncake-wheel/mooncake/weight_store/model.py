from __future__ import annotations

import hashlib
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .model_keyspace import (
    model_file_chunk_key,
    model_file_key,
    model_id_index_key,
    model_index_key,
    model_manifest_key,
)

SCHEMA_VERSION = 1
READY = "READY"
FAILED = "FAILED"
VALID_STATUSES = {READY, FAILED}
DEFAULT_FILE_CHUNK_SIZE = 64 * 1024 * 1024

WEIGHT_SUFFIXES = {
    ".bin",
    ".gguf",
    ".pt",
    ".pth",
    ".safetensors",
}


@dataclass(frozen=True)
class ModelFileRecord:
    path: str
    type: str
    key: str
    size: int
    sha256: str
    chunks: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ModelFileManifest:
    checkpoint_id: str
    model_id: str
    revision: str
    status: str
    source_uri: str
    created_at: int
    updated_at: int
    total_size: int
    files: List[ModelFileRecord]
    error: Optional[str]
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(f"invalid model status: {self.status!r}")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> "ModelFileManifest":
        data = json.loads(payload.decode("utf-8"))
        data["files"] = [
            ModelFileRecord(chunks=[item["key"]], **item)
            if "chunks" not in item
            else ModelFileRecord(**item)
            for item in data["files"]
        ]
        return cls(**data)


class ModelFileCacheClient:
    def __init__(
        self,
        store,
        *,
        replica_num: int = 1,
        hard_pin_weights: bool = True,
        hard_pin_metadata: bool = True,
        file_chunk_size: int = DEFAULT_FILE_CHUNK_SIZE,
        progress: bool = False,
    ) -> None:
        if file_chunk_size <= 0:
            raise ValueError("file_chunk_size must be positive")
        self.store = store
        self.replica_num = replica_num
        self.hard_pin_weights = hard_pin_weights
        self.hard_pin_metadata = hard_pin_metadata
        self.file_chunk_size = file_chunk_size
        self.progress = progress
        self.file_key = model_file_key

    def import_model(
        self,
        *,
        checkpoint_id: str,
        model_id: str,
        revision: str,
        source_uri: str,
    ) -> ModelFileManifest:
        source = Path(source_uri)
        if not source.is_dir():
            raise ValueError(f"source_uri must be a directory: {source_uri}")
        if self._exists(model_manifest_key(checkpoint_id)):
            raise ValueError(f"model checkpoint already exists: {checkpoint_id}")

        files = _list_model_files(source)
        if not files:
            raise ValueError(f"model directory has no files: {source_uri}")
        _validate_safetensors_index(source, files)

        now = int(time.time())
        records: List[ModelFileRecord] = []
        total_bytes = sum(path.stat().st_size for path in files)
        imported_bytes = 0
        import_started_at = time.time()
        try:
            for index, path in enumerate(files, start=1):
                rel = path.relative_to(source).as_posix()
                file_type = _file_type(rel)
                key = self.file_key(checkpoint_id, rel)
                self._log_progress(
                    self._format_progress(
                        "start",
                        rel,
                        index,
                        len(files),
                        imported_bytes,
                        total_bytes,
                        import_started_at,
                    )
                )
                size, digest, chunks = self._put_file_chunks(
                    checkpoint_id=checkpoint_id,
                    relative_path=rel,
                    source_path=path,
                    file_type=file_type,
                )
                records.append(
                    ModelFileRecord(
                        path=rel,
                        type=file_type,
                        key=key,
                        size=size,
                        sha256=digest,
                        chunks=chunks,
                    )
                )
                imported_bytes += size
                self._log_progress(
                    self._format_progress(
                        "done",
                        rel,
                        index,
                        len(files),
                        imported_bytes,
                        total_bytes,
                        import_started_at,
                        chunks=len(chunks),
                    )
                )

            manifest = ModelFileManifest(
                checkpoint_id=checkpoint_id,
                model_id=model_id,
                revision=revision,
                status=READY,
                source_uri=str(source),
                created_at=now,
                updated_at=now,
                total_size=sum(record.size for record in records),
                files=sorted(records, key=lambda record: record.path),
                error=None,
            )
            self._write_manifest(manifest)
            self._add_to_indexes(manifest)
            self.verify_model(checkpoint_id)
            return manifest
        except BaseException as exc:
            failed = ModelFileManifest(
                checkpoint_id=checkpoint_id,
                model_id=model_id,
                revision=revision,
                status=FAILED,
                source_uri=str(source),
                created_at=now,
                updated_at=int(time.time()),
                total_size=sum(record.size for record in records),
                files=sorted(records, key=lambda record: record.path),
                error=repr(exc),
            )
            self._write_manifest(failed)
            raise

    def list_models(self) -> List[str]:
        return sorted(self._read_index(model_index_key()))

    def inspect_model(self, checkpoint_id: str) -> ModelFileManifest:
        return self._read_manifest(checkpoint_id)

    def verify_model(self, checkpoint_id: str) -> ModelFileManifest:
        manifest = self._read_manifest(checkpoint_id)
        if manifest.status != READY:
            raise ValueError(
                f"model checkpoint {checkpoint_id!r} is not READY: "
                f"{manifest.status}"
            )

        by_path = {record.path for record in manifest.files}
        payload_by_path: Dict[str, bytes] = {}
        for record in manifest.files:
            size, digest, payload = self._read_file_chunks(
                record, keep_payload=(record.path == "model.safetensors.index.json")
            )
            if size != record.size:
                raise ValueError(
                    f"file size mismatch for {record.path}: "
                    f"expected {record.size}, got {size}"
                )
            if digest != record.sha256:
                raise ValueError(
                    f"sha256 mismatch for {record.path}: "
                    f"expected {record.sha256}, got {digest}"
                )
            if payload is not None:
                payload_by_path[record.path] = payload
        if "model.safetensors.index.json" in payload_by_path:
            index = json.loads(
                payload_by_path["model.safetensors.index.json"].decode("utf-8")
            )
            _validate_weight_map(index.get("weight_map", {}), by_path)
        return manifest

    def delete_model(self, checkpoint_id: str) -> None:
        manifest = self._read_manifest(checkpoint_id)
        for record in manifest.files:
            self._remove_file_chunks(record)
        self._remove(model_manifest_key(checkpoint_id), force=True)
        self._remove_from_indexes(manifest)

    def materialize_file(self, checkpoint_id: str, path: str, output_path: str) -> None:
        manifest = self._read_manifest(checkpoint_id)
        matches = [record for record in manifest.files if record.path == path]
        if not matches:
            raise KeyError(path)
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        record = matches[0]
        digest = hashlib.sha256()
        size = 0
        with output.open("wb") as output_file:
            for chunk_key in self._record_chunks(record):
                chunk = self._get(chunk_key)
                output_file.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        if size != record.size:
            raise ValueError(
                f"file size mismatch for {record.path}: "
                f"expected {record.size}, got {size}"
            )
        if digest.hexdigest() != record.sha256:
            raise ValueError(f"sha256 mismatch for {record.path}")

    def _write_manifest(self, manifest: ModelFileManifest) -> None:
        self._put_control(
            model_manifest_key(manifest.checkpoint_id),
            manifest.to_json_bytes(),
            self._new_config("METADATA", hard_pin=self.hard_pin_metadata),
        )

    def _read_manifest(self, checkpoint_id: str) -> ModelFileManifest:
        key = model_manifest_key(checkpoint_id)
        payload = self._get(key)
        if not payload:
            raise KeyError(
                f"model checkpoint {checkpoint_id!r} has no readable manifest"
            )
        try:
            return ModelFileManifest.from_json_bytes(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"model checkpoint {checkpoint_id!r} manifest is not valid JSON"
            ) from exc

    def _add_to_indexes(self, manifest: ModelFileManifest) -> None:
        all_models = self._read_index(model_index_key())
        all_models.add(manifest.checkpoint_id)
        self._write_index(model_index_key(), all_models)

        per_model = self._read_index(model_id_index_key(manifest.model_id))
        per_model.add(manifest.checkpoint_id)
        self._write_index(model_id_index_key(manifest.model_id), per_model)

    def _remove_from_indexes(self, manifest: ModelFileManifest) -> None:
        all_models = self._read_index(model_index_key())
        all_models.discard(manifest.checkpoint_id)
        self._write_index(model_index_key(), all_models)

        per_model = self._read_index(model_id_index_key(manifest.model_id))
        per_model.discard(manifest.checkpoint_id)
        self._write_index(model_id_index_key(manifest.model_id), per_model)

    def _read_index(self, key: str) -> set[str]:
        if not self._exists(key):
            return set()
        value = self.store.get(key)
        if value is None:
            return set()
        if isinstance(value, str):
            payload = value.encode("utf-8")
        else:
            payload = bytes(value)
        if not payload:
            return set()
        return set(json.loads(payload.decode("utf-8")))

    def _write_index(self, key: str, values: set[str]) -> None:
        self._put_control(
            key,
            json.dumps(sorted(values), separators=(",", ":")).encode("utf-8"),
            self._new_config("METADATA", hard_pin=self.hard_pin_metadata),
        )

    def _new_config(self, data_type_name: str, *, hard_pin: bool):
        config_cls = getattr(self.store, "ReplicateConfig", None)
        data_type_cls = getattr(self.store, "ObjectDataType", None)
        if config_cls is None:
            from mooncake.store import ReplicateConfig

            config_cls = ReplicateConfig
        if data_type_cls is None:
            try:
                from mooncake.store import ObjectDataType

                data_type_cls = ObjectDataType
            except ImportError:
                data_type_cls = None

        config = config_cls()
        config.replica_num = self.replica_num
        config.with_hard_pin = hard_pin
        if data_type_cls is not None and hasattr(config, "data_type"):
            config.data_type = getattr(data_type_cls, data_type_name)
        return config

    def _put_file_chunks(
        self,
        *,
        checkpoint_id: str,
        relative_path: str,
        source_path: Path,
        file_type: str,
    ) -> tuple[int, str, List[str]]:
        digest = hashlib.sha256()
        size = 0
        chunks: List[str] = []
        config = self._new_config(
            file_type,
            hard_pin=(
                self.hard_pin_weights
                if file_type == "WEIGHT"
                else self.hard_pin_metadata
            ),
        )
        with source_path.open("rb") as source_file:
            try:
                for chunk_index, chunk in enumerate(
                    iter(lambda: source_file.read(self.file_chunk_size), b"")
                ):
                    chunk_key = model_file_chunk_key(
                        checkpoint_id, relative_path, chunk_index
                    )
                    self._put(chunk_key, chunk, config)
                    chunks.append(chunk_key)
                    digest.update(chunk)
                    size += len(chunk)
            except BaseException:
                for chunk_key in chunks:
                    self._remove(chunk_key, force=True)
                raise
        return size, digest.hexdigest(), chunks

    def _read_file_chunks(
        self, record: ModelFileRecord, *, keep_payload: bool
    ) -> tuple[int, str, Optional[bytes]]:
        digest = hashlib.sha256()
        size = 0
        payload_chunks: List[bytes] = []
        for chunk_key in self._record_chunks(record):
            chunk = self._get(chunk_key)
            digest.update(chunk)
            size += len(chunk)
            if keep_payload:
                payload_chunks.append(chunk)
        payload = b"".join(payload_chunks) if keep_payload else None
        return size, digest.hexdigest(), payload

    def _remove_file_chunks(self, record: ModelFileRecord) -> None:
        for chunk_key in self._record_chunks(record):
            self._remove(chunk_key, force=True)

    def _record_chunks(self, record: ModelFileRecord) -> List[str]:
        if record.size == 0:
            return []
        if record.chunks:
            return record.chunks
        return [record.key]

    def _put(self, key: str, value: bytes, config) -> None:
        result = self.store.put(key, value, config)
        if result not in (0, None):
            raise RuntimeError(f"failed to put {key}: {result}")

    def _put_control(self, key: str, value: bytes, config) -> None:
        writer = getattr(self.store, "upsert", None)
        if writer is None:
            writer = self.store.put
        result = writer(key, value, config)
        if result not in (0, None):
            raise RuntimeError(f"failed to write control object {key}: {result}")

    def _get(self, key: str) -> bytes:
        value = self.store.get(key)
        if value is None:
            raise KeyError(key)
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(value)

    def _remove(self, key: str, *, force: bool) -> None:
        try:
            result = self.store.remove(key, force)
        except TypeError:
            result = self.store.remove(key)
        if result not in (0, None):
            raise RuntimeError(f"failed to remove {key}: {result}")

    def _exists(self, key: str) -> bool:
        checker = getattr(self.store, "is_exist", None)
        if checker is not None:
            result = checker(key)
            if result < 0:
                raise RuntimeError(f"failed to check {key}: {result}")
            return bool(result)
        return self.store.get(key) is not None

    def _log_progress(self, message: str) -> None:
        if self.progress:
            print(f"[weight-store] {message}", file=sys.stderr, flush=True)

    def _format_progress(
        self,
        phase: str,
        relative_path: str,
        file_index: int,
        file_count: int,
        imported_bytes: int,
        total_bytes: int,
        started_at: float,
        *,
        chunks: Optional[int] = None,
    ) -> str:
        elapsed = max(time.time() - started_at, 1e-6)
        rate = imported_bytes / elapsed if imported_bytes else 0
        percent = (imported_bytes / total_bytes * 100) if total_bytes else 0
        eta = (total_bytes - imported_bytes) / rate if rate > 0 else None
        details = [
            f"{phase} file {file_index}/{file_count}",
            relative_path,
            f"{_format_bytes(imported_bytes)}/{_format_bytes(total_bytes)}",
            f"{percent:.1f}%",
        ]
        if rate > 0:
            details.append(f"{_format_bytes(rate)}/s")
        if eta is not None:
            details.append(f"eta {_format_duration(eta)}")
        if chunks is not None:
            details.append(f"{chunks} chunks")
        return " | ".join(details)


def _list_model_files(source: Path) -> List[Path]:
    return sorted(path for path in source.rglob("*") if path.is_file())


def _file_type(relative_path: str) -> str:
    suffix = Path(relative_path).suffix.lower()
    return "WEIGHT" if suffix in WEIGHT_SUFFIXES else "METADATA"


def _validate_safetensors_index(source: Path, files: List[Path]) -> None:
    relative_files = {path.relative_to(source).as_posix() for path in files}
    index_path = source / "model.safetensors.index.json"
    if not index_path.exists():
        return
    data = json.loads(index_path.read_text(encoding="utf-8"))
    _validate_weight_map(data.get("weight_map", {}), relative_files)


def _validate_weight_map(weight_map: Dict[str, str], relative_files: set[str]) -> None:
    missing = sorted(
        {path for path in weight_map.values() if path not in relative_files}
    )
    if missing:
        raise ValueError(
            "safetensors index references missing shard(s): " + ", ".join(missing)
        )


def _format_bytes(value: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(value)
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024
    raise AssertionError("unreachable")


def _format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"
