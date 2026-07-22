# SPDX-License-Identifier: Apache-2.0
"""Mooncake Store connector for remote model weight save/load.

Ported from SGLang ``srt/connector/mooncake_store.py`` so engines can depend on
Mooncake's first-party implementation instead of maintaining a fork.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Generator, List, Optional, Tuple

import torch

from mooncake.model_loader.config import (
    COMMON_REMOTE_MODEL_FILES,
    MooncakeStoreLoaderConfig,
    parse_model_name,
)
from mooncake.model_loader.connector_base import BaseKVConnector

logger = logging.getLogger(__name__)

FILE_INDEX_KEY_PREFIX = "__index__"
STANDALONE_CHUNK_SIZE = 8 * 1024 * 1024


def _tensor_to_bytes(tensor: torch.Tensor) -> bytes:
    return (
        tensor.detach()
        .to("cpu")
        .contiguous()
        .view(torch.uint8)
        .numpy()
        .tobytes()
    )


def _copy_bytes_into_tensor(data: bytes, tensor: torch.Tensor) -> None:
    tensor_size = tensor.untyped_storage().nbytes()
    if len(data) != tensor_size:
        raise RuntimeError(
            f"Expected {tensor_size} bytes for tensor load, got {len(data)}"
        )

    source = torch.frombuffer(bytearray(data), dtype=torch.uint8)
    target = tensor.view(torch.uint8).reshape(-1)
    if target.device.type == "cpu":
        target.copy_(source)
    else:
        target.copy_(source.to(target.device))


def _chunk_manifest_key(key: str) -> str:
    return f"{key}.__chunk_manifest__"


def _chunk_data_key(key: str, chunk_idx: int) -> str:
    return f"{key}.__chunk__.{chunk_idx}"


def _check_batch_get_results(
    keys: List[str], results: List[int], tensor_sizes: List[int]
) -> None:
    if len(results) != len(keys):
        raise RuntimeError(
            "Mooncake batch_get_into returned %d results for %d keys"
            % (len(results), len(keys))
        )

    failures = []
    for key, result, tensor_size in zip(keys, results, tensor_sizes):
        if result < 0:
            failures.append(f"{key}: error={result}")
        elif result != tensor_size:
            failures.append(f"{key}: expected {tensor_size} bytes, got {result}")

    if failures:
        raise RuntimeError(
            "Mooncake batch_get_into failed for some tensors: "
            + "; ".join(failures)
        )


def _check_batch_put_results(keys: List[str], results: List[int]) -> None:
    if len(results) != len(keys):
        raise RuntimeError(
            "Mooncake batch_put_from returned %d results for %d keys"
            % (len(results), len(keys))
        )

    failures = [
        f"{key}: error={result}"
        for key, result in zip(keys, results)
        if result != 0
    ]
    if failures:
        raise RuntimeError(
            "Mooncake batch_put_from failed for some tensors: "
            + "; ".join(failures)
        )


def pull_files_from_db(
    connector: "MooncakeStoreConnector",
    model_name: str,
    allow_pattern: Optional[List[str]] = None,
    ignore_pattern: Optional[List[str]] = None,
) -> None:
    """Materialize sidecar text files from Store into ``connector.local_dir``."""
    del allow_pattern, ignore_pattern  # reserved for future filtering
    prefix = f"{model_name}/files/"
    local_dir = connector.get_local_dir()
    files = set(connector.list(prefix))

    for file_name in COMMON_REMOTE_MODEL_FILES:
        remote_file = f"{prefix}{file_name}"
        if not connector._key_exists(remote_file):
            continue
        content = connector.getstr(remote_file)
        if content is not None:
            files.add(remote_file)

    for file in sorted(files):
        destination_file = os.path.join(local_dir, file.removeprefix(prefix))
        Path(destination_file).parent.mkdir(parents=True, exist_ok=True)
        content = connector.getstr(file)
        if content is None:
            continue
        with open(destination_file, "wb") as f:
            f.write(content.encode("utf-8"))


class MooncakeStoreConnector(BaseKVConnector):
    """KV connector backed by ``MooncakeDistributedStore``.

    URL format::

        mooncake:///<model_name>
    """

    def __init__(
        self,
        url: str,
        *,
        config: Optional[MooncakeStoreLoaderConfig] = None,
        store: Any = None,
    ):
        super().__init__(url)
        self.model_name = parse_model_name(url)
        self.config = config or MooncakeStoreLoaderConfig.load_from_env()

        if store is not None:
            self.store = store
        else:
            self.store = self._create_and_setup_store()

        try:
            from mooncake.store import ReplicateConfig
        except ImportError as e:
            raise ImportError(
                "mooncake.store (C++ extension) is required for "
                "MooncakeStoreConnector"
            ) from e

        self._rep_config = ReplicateConfig()
        self._rep_config.replica_num = self.config.replica_num
        if self.config.preferred_segments:
            self._rep_config.preferred_segments = list(
                self.config.preferred_segments
            )

        logger.info(
            "MooncakeStoreConnector initialized for model %s", self.model_name
        )

    def _create_and_setup_store(self) -> Any:
        try:
            from mooncake.store import MooncakeDistributedStore
        except ImportError as e:
            raise ImportError(
                "Please install mooncake-transfer-engine with the Store "
                "extension to use MooncakeStoreConnector."
            ) from e

        store = MooncakeDistributedStore()
        if self.config.standalone_storage:
            if not self.config.client_server_address:
                raise ValueError(
                    "MOONCAKE_CLIENT must be set when "
                    "MOONCAKE_STANDALONE_STORAGE is enabled."
                )
            ret_code = store.setup_dummy(
                self.config.global_segment_size,
                self.config.local_buffer_size,
                self.config.client_server_address,
            )
        else:
            ret_code = store.setup(
                self.config.local_hostname,
                self.config.metadata_server,
                self.config.global_segment_size,
                self.config.local_buffer_size,
                self.config.protocol,
                self.config.device_name,
                self.config.master_server_address,
            )
        if ret_code:
            raise RuntimeError(
                f"Failed to setup Mooncake store, error code: {ret_code}"
            )
        return store

    def _full_key(self, key: str) -> str:
        if key.startswith(f"{self.model_name}/") or key.startswith(
            FILE_INDEX_KEY_PREFIX
        ):
            return key
        return f"{self.model_name}/{key}"

    def _key_exists(self, key: str) -> bool:
        ret_code = self.store.is_exist(key)
        if ret_code < 0:
            raise RuntimeError(
                f"Failed to query Mooncake key '{key}', error code: {ret_code}"
            )
        return ret_code == 1

    def batch_get_into(self, keys: List[str], tensors: List[torch.Tensor]) -> None:
        full_keys = [self._full_key(key) for key in keys]

        if self.config.standalone_storage:
            for full_key, tensor in zip(full_keys, tensors):
                if self._key_exists(full_key):
                    data = self.store.get(full_key)
                else:
                    manifest_key = _chunk_manifest_key(full_key)
                    if not self._key_exists(manifest_key):
                        raise RuntimeError(f"Mooncake key not found: {full_key}")
                    manifest = json.loads(
                        self.store.get(manifest_key).decode("utf-8")
                    )
                    data = b"".join(
                        self.store.get(_chunk_data_key(full_key, chunk_idx))
                        for chunk_idx in range(manifest["num_chunks"])
                    )
                _copy_bytes_into_tensor(data, tensor)
            return

        tensor_ptrs = [tensor.data_ptr() for tensor in tensors]
        tensor_sizes = [tensor.untyped_storage().nbytes() for tensor in tensors]

        for tensor in tensors:
            ret_code = self.store.register_buffer(
                tensor.data_ptr(), tensor.untyped_storage().nbytes()
            )
            if ret_code:
                raise RuntimeError(
                    "Failed to register buffer to Mooncake Store, "
                    f"error code: {ret_code}"
                )

        results = self.store.batch_get_into(full_keys, tensor_ptrs, tensor_sizes)
        _check_batch_get_results(full_keys, results, tensor_sizes)

    def get_into(self, key: str, tensor: torch.Tensor) -> None:
        self.batch_get_into([key], [tensor])

    def get(self, key: str) -> Optional[torch.Tensor]:
        raise NotImplementedError(
            "Use batch_get_into() instead for performance."
        )

    def getstr(self, key: str) -> Optional[str]:
        if not self._key_exists(key):
            logger.error("Key %s not found in Mooncake store", key)
            return None
        data = self.store.get(key)
        return data.decode("utf-8")

    def set(self, key: str, tensor: torch.Tensor) -> None:
        raise NotImplementedError(
            "Use batch_put_from() instead for performance."
        )

    def batch_put_from(self, keys: List[str], tensors: List[torch.Tensor]) -> None:
        # Callers using RemoteWeightIO pass fully-qualified keys
        # (already include model_name/). Relative keys are also accepted.
        full_keys = [self._full_key(key) for key in keys]

        if self.config.standalone_storage:
            failures = []
            for key, tensor in zip(full_keys, tensors):
                data = _tensor_to_bytes(tensor)
                if len(data) <= STANDALONE_CHUNK_SIZE:
                    ret_code = self.store.put(key, data, self._rep_config)
                    if ret_code != 0:
                        failures.append(f"{key}: error={ret_code}")
                    continue

                num_chunks = (
                    len(data) + STANDALONE_CHUNK_SIZE - 1
                ) // STANDALONE_CHUNK_SIZE
                for chunk_idx in range(num_chunks):
                    start = chunk_idx * STANDALONE_CHUNK_SIZE
                    end = start + STANDALONE_CHUNK_SIZE
                    ret_code = self.store.put(
                        _chunk_data_key(key, chunk_idx),
                        data[start:end],
                        self._rep_config,
                    )
                    if ret_code != 0:
                        failures.append(
                            f"{_chunk_data_key(key, chunk_idx)}: error={ret_code}"
                        )
                        break
                else:
                    ret_code = self.store.put(
                        _chunk_manifest_key(key),
                        json.dumps(
                            {"num_chunks": num_chunks, "size": len(data)}
                        ).encode("utf-8"),
                        self._rep_config,
                    )
                    if ret_code != 0:
                        failures.append(
                            f"{_chunk_manifest_key(key)}: error={ret_code}"
                        )
            if failures:
                raise RuntimeError(
                    "Mooncake put failed for some tensors: "
                    + "; ".join(failures)
                )
            return

        tensor_ptrs = [tensor.data_ptr() for tensor in tensors]
        tensor_sizes = [tensor.untyped_storage().nbytes() for tensor in tensors]

        for tensor in tensors:
            ret_code = self.store.register_buffer(
                tensor.data_ptr(), tensor.untyped_storage().nbytes()
            )
            if ret_code:
                raise RuntimeError(
                    "Failed to register buffer to Mooncake Store, "
                    f"error code: {ret_code}"
                )

        results = self.store.batch_put_from(
            full_keys, tensor_ptrs, tensor_sizes, self._rep_config
        )
        _check_batch_put_results(full_keys, results)

    def setstr(self, key: str, obj: str) -> None:
        ret_code = self.store.put(key, obj.encode("utf-8"), self._rep_config)
        if ret_code != 0:
            raise RuntimeError(
                f"Failed to put string key '{key}' into Mooncake store, "
                f"error code: {ret_code}"
            )

        prefix, _, _ = key.rpartition("/")
        self._register_key_in_index(key, f"{prefix}/")

    def list(self, prefix: str) -> List[str]:
        index_key = f"{FILE_INDEX_KEY_PREFIX}{prefix}"
        if not self._key_exists(index_key):
            return []

        data = self.store.get(index_key)
        content = data.decode("utf-8").strip()
        if not content:
            return []
        return content.split("\n")

    def _register_key_in_index(self, key: str, prefix: str) -> None:
        index_key = f"{FILE_INDEX_KEY_PREFIX}{prefix}"
        if not self._key_exists(index_key):
            keys_list = [key]
        else:
            existing = self.store.get(index_key)
            keys_list = existing.decode("utf-8").strip().split("\n")
            if key not in keys_list:
                keys_list.append(key)
        ret_code = self.store.put(
            index_key, "\n".join(keys_list).encode("utf-8"), self._rep_config
        )
        if ret_code != 0:
            raise RuntimeError(
                f"Failed to update Mooncake file index '{index_key}', "
                f"error code: {ret_code}"
            )

    def weight_iterator(
        self, rank: int = 0
    ) -> Generator[Tuple[str, torch.Tensor], None, None]:
        raise NotImplementedError(
            "MooncakeStoreConnector does not support iterating weights one by "
            "one. Use batch_get_into via RemoteWeightIO instead."
        )

    def pull_files(
        self,
        allow_pattern: Optional[List[str]] = None,
        ignore_pattern: Optional[List[str]] = None,
    ) -> None:
        pull_files_from_db(self, self.model_name, allow_pattern, ignore_pattern)


def create_mooncake_connector(
    url: str,
    *,
    config: Optional[MooncakeStoreLoaderConfig] = None,
    store: Any = None,
) -> MooncakeStoreConnector:
    """Factory for ``mooncake:///<model_name>`` connectors."""
    return MooncakeStoreConnector(url, config=config, store=store)
