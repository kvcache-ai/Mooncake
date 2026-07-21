# SPDX-License-Identifier: Apache-2.0
"""Seed Mooncake Store with HF safetensors using the remote weight key layout."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urlparse

from mooncake.model_loader.config import (
    COMMON_REMOTE_MODEL_FILES,
    SAVE_MODEL_DEFAULT_BATCH_SIZE,
    parse_model_name,
)
from mooncake.model_loader.mooncake_store_connector import (
    MooncakeStoreConnector,
    create_mooncake_connector,
)
from mooncake.store_file_io import _open_fs_target

logger = logging.getLogger(__name__)


def _load_safetensors_mapping(payload: bytes) -> Dict[str, Any]:
    try:
        from safetensors.torch import load as st_load
    except ImportError as e:
        raise ImportError(
            "Install mooncake-transfer-engine[file-io] (safetensors+torch) "
            "to seed weights from safetensors files."
        ) from e
    return dict(st_load(payload))


def _is_remote(path: str) -> bool:
    scheme = urlparse(path).scheme
    return bool(scheme) and scheme not in {"", "file"}


def _join_root(root: str, name: str) -> str:
    if _is_remote(root):
        return root.rstrip("/") + "/" + name.lstrip("/")
    return os.path.join(root, name)


def _read_bytes(path_or_uri: str, storage_options: Optional[Mapping[str, Any]]) -> bytes:
    fs, fs_path = _open_fs_target(path_or_uri, "auto", dict(storage_options or {}))
    with fs.open(fs_path, "rb") as handle:
        return handle.read()


def _basename(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


def _resolve_weight_files(
    model_root: str,
    storage_options: Optional[Mapping[str, Any]],
) -> List[str]:
    fs, fs_path = _open_fs_target(model_root, "auto", dict(storage_options or {}))

    # Single safetensors file
    if str(model_root).endswith(".safetensors") or str(fs_path).endswith(
        ".safetensors"
    ):
        return [model_root]

    index_path = fs_path.rstrip("/") + "/model.safetensors.index.json"
    if fs.exists(index_path):
        with fs.open(index_path, "rb") as handle:
            index = json.loads(handle.read().decode("utf-8"))
        weight_map = index.get("weight_map") or {}
        shard_names = sorted(set(weight_map.values()))
        # model_root is the checkpoint directory
        root = model_root
        if str(model_root).endswith(".json"):
            root = os.path.dirname(model_root.rstrip("/"))
        return [_join_root(root, name) for name in shard_names]

    if fs.isdir(fs_path):
        entries = fs.ls(fs_path, detail=False)
        shards = [e for e in entries if str(e).endswith(".safetensors")]
        if not shards:
            raise FileNotFoundError(
                f"No safetensors files found under {model_root}"
            )
        return [_join_root(model_root, _basename(s)) for s in shards]

    raise FileNotFoundError(f"Cannot resolve weight files at {model_root}")


def seed_model_from_files(
    model_root: str,
    url: str,
    *,
    tp_rank: int = 0,
    storage_options: Optional[Mapping[str, Any]] = None,
    connector: Optional[MooncakeStoreConnector] = None,
    batch_size: int = SAVE_MODEL_DEFAULT_BATCH_SIZE,
    include_sidecar_files: bool = True,
) -> None:
    """Load safetensors from ``model_root`` into Store under ``url`` key layout.

    Writes ``{model}/keys/rank_{tp_rank}/{param}`` for each tensor. When
    ``model_root`` is a local directory, also stores common sidecar text files
    under ``{model}/files/``.
    """
    model_name = parse_model_name(url)
    owns = connector is None
    client = connector or create_mooncake_connector(url)

    try:
        weight_files = _resolve_weight_files(model_root, storage_options)
        batch_keys: list[str] = []
        batch_tensors: list[Any] = []

        for weight_file in weight_files:
            payload = _read_bytes(weight_file, storage_options)
            tensors = _load_safetensors_mapping(payload)
            for name, tensor in tensors.items():
                r_key = f"{model_name}/keys/rank_{tp_rank}/{name}"
                batch_keys.append(r_key)
                batch_tensors.append(tensor)
                if len(batch_keys) >= batch_size:
                    client.batch_put_from(batch_keys, batch_tensors)
                    batch_keys.clear()
                    batch_tensors.clear()

        if batch_keys:
            client.batch_put_from(batch_keys, batch_tensors)

        if include_sidecar_files and os.path.isdir(model_root):
            for file_name in COMMON_REMOTE_MODEL_FILES:
                path = os.path.join(model_root, file_name)
                if not os.path.isfile(path):
                    continue
                try:
                    with open(path, encoding="utf-8") as handle:
                        content = handle.read()
                except UnicodeDecodeError:
                    continue
                client.setstr(f"{model_name}/files/{file_name}", content)

        logger.info(
            "Seeded Mooncake Store model %s from %s (rank=%s, shards=%d)",
            model_name,
            model_root,
            tp_rank,
            len(weight_files),
        )
    finally:
        if owns:
            client.close()
