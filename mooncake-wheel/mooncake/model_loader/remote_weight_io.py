# SPDX-License-Identifier: Apache-2.0
"""Engine-agnostic remote weight save/load (SGLang RemoteModelLoader semantics)."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Mapping, Optional

import torch
import torch.nn as nn

from mooncake.model_loader.config import (
    COMMON_REMOTE_MODEL_FILES,
    LOAD_MODEL_DEFAULT_BATCH_SIZE,
    SAVE_MODEL_DEFAULT_BATCH_SIZE,
    parse_model_name,
    relative_rank_weight_key,
)
from mooncake.model_loader.mooncake_store_connector import (
    MooncakeStoreConnector,
    create_mooncake_connector,
)

logger = logging.getLogger(__name__)


def filter_subtensors(state_dict: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
    """Drop tensors that are views into another tensor's storage.

    Mirrors SGLang/vLLM ``ShardedStateLoader._filter_subtensors`` enough for
    remote save/load (keeps the tensor with the largest numel per storage).
    """
    same_storage: Dict[int, list[tuple[str, torch.Tensor]]] = {}
    for name, tensor in state_dict.items():
        key = tensor.untyped_storage().data_ptr()
        same_storage.setdefault(key, []).append((name, tensor))

    filtered: Dict[str, torch.Tensor] = {}
    for group in same_storage.values():
        if len(group) == 1:
            name, tensor = group[0]
            filtered[name] = tensor
            continue
        # Keep the widest tensor for each shared storage.
        name, tensor = max(group, key=lambda item: item[1].numel())
        filtered[name] = tensor
    return filtered


class RemoteWeightIO:
    """Save/load nn.Module weights through a Mooncake Store connector."""

    def __init__(
        self,
        url: str,
        *,
        connector: Optional[MooncakeStoreConnector] = None,
        batch_size: int = SAVE_MODEL_DEFAULT_BATCH_SIZE,
    ):
        self.url = url
        self.model_name = parse_model_name(url)
        self.batch_size = batch_size
        self._owns_connector = connector is None
        self.connector = connector or create_mooncake_connector(url)

    def close(self) -> None:
        if self._owns_connector:
            self.connector.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def save_model(
        self,
        model: nn.Module,
        model_path: Optional[str] = None,
        *,
        rank: int = 0,
        state_dict: Optional[Mapping[str, torch.Tensor]] = None,
    ) -> None:
        """Persist filtered ``state_dict`` (+ optional sidecar files) to Store."""
        tensors = filter_subtensors(
            dict(state_dict) if state_dict is not None else model.state_dict()
        )
        batch_keys: list[str] = []
        batch_tensors: list[torch.Tensor] = []

        for key, tensor in tensors.items():
            r_key = f"{self.model_name}/keys/rank_{rank}/{key}"
            batch_keys.append(r_key)
            batch_tensors.append(tensor)
            if len(batch_keys) >= self.batch_size:
                self.connector.batch_put_from(batch_keys, batch_tensors)
                batch_keys.clear()
                batch_tensors.clear()

        if batch_keys:
            self.connector.batch_put_from(batch_keys, batch_tensors)

        if model_path:
            self._save_sidecar_files(model_path)

    def _save_sidecar_files(self, model_path: str) -> None:
        for root, _, files in os.walk(model_path):
            for file_name in files:
                if file_name.startswith("."):
                    continue
                if (
                    os.path.splitext(file_name)[1] in (".json", ".py")
                    or file_name in COMMON_REMOTE_MODEL_FILES
                ):
                    file_path = os.path.join(root, file_name)
                    try:
                        with open(file_path, encoding="utf-8") as file:
                            file_content = file.read()
                    except UnicodeDecodeError:
                        logger.warning("Skipping non-utf8 sidecar file %s", file_path)
                        continue
                    f_key = f"{self.model_name}/files/{file_name}"
                    self.connector.setstr(f_key, file_content)

    def load_weights_into(
        self,
        model: nn.Module,
        *,
        rank: int = 0,
        state_dict: Optional[Dict[str, torch.Tensor]] = None,
        batch_size: int = LOAD_MODEL_DEFAULT_BATCH_SIZE,
    ) -> None:
        """Fill ``model`` parameters in-place from Store via ``batch_get_into``."""
        tensors = filter_subtensors(
            state_dict if state_dict is not None else model.state_dict()
        )
        batch_keys: list[str] = []
        batch_tensors: list[torch.Tensor] = []

        for key, tensor in tensors.items():
            r_key = relative_rank_weight_key(rank, key)
            batch_keys.append(r_key)
            batch_tensors.append(tensor)
            if len(batch_keys) >= batch_size:
                self.connector.batch_get_into(batch_keys, batch_tensors)
                batch_keys.clear()
                batch_tensors.clear()

        if batch_keys:
            self.connector.batch_get_into(batch_keys, batch_tensors)

    def materialize_files(self) -> str:
        """Pull sidecar files into the connector scratch dir; return its path."""
        self.connector.pull_files()
        return self.connector.get_local_dir()


def save_model(
    model: nn.Module,
    url: str,
    model_path: Optional[str] = None,
    *,
    rank: int = 0,
    connector: Optional[MooncakeStoreConnector] = None,
) -> None:
    with RemoteWeightIO(url, connector=connector) as io:
        io.save_model(model, model_path, rank=rank)


def load_weights_into(
    model: nn.Module,
    url: str,
    *,
    rank: int = 0,
    connector: Optional[MooncakeStoreConnector] = None,
) -> None:
    with RemoteWeightIO(url, connector=connector) as io:
        io.load_weights_into(model, rank=rank)
