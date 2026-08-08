from __future__ import annotations

import hashlib
import re


_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def model_manifest_key(checkpoint_id: str) -> str:
    _validate_id("checkpoint_id", checkpoint_id)
    return f"weight/models/{checkpoint_id}/manifest"


def model_file_key(checkpoint_id: str, relative_path: str) -> str:
    _validate_id("checkpoint_id", checkpoint_id)
    return f"weight/models/{checkpoint_id}/files/{_sha256_text(relative_path)}"


def model_file_chunk_key(
    checkpoint_id: str, relative_path: str, chunk_index: int
) -> str:
    if chunk_index < 0:
        raise ValueError(f"invalid chunk_index: {chunk_index!r}")
    return f"{model_file_key(checkpoint_id, relative_path)}/chunks/{chunk_index:08d}"


def model_index_key() -> str:
    return "weight/index/models"


def model_id_index_key(model_id: str) -> str:
    return f"weight/index/model/{_sha256_text(model_id)}"


def validate_checkpoint_id(checkpoint_id: str) -> None:
    _validate_id("checkpoint_id", checkpoint_id)


def _sha256_text(value: str) -> str:
    if not value:
        raise ValueError("value must not be empty")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _validate_id(name: str, value: str) -> None:
    if not _ID_RE.fullmatch(value):
        raise ValueError(f"invalid {name}: {value!r}")
