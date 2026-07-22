# SPDX-License-Identifier: Apache-2.0
"""Configuration for Mooncake remote model loaders."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from mooncake.mooncake_config import (
    DEFAULT_GLOBAL_SEGMENT_SIZE,
    MooncakeConfig,
    _parse_bool,
    _parse_segment_size,
)

DEFAULT_CONNECTOR_LOCAL_BUFFER_SIZE = 16 * 1024 * 1024  # 16 MiB
SAVE_MODEL_DEFAULT_BATCH_SIZE = 256
LOAD_MODEL_DEFAULT_BATCH_SIZE = 256

COMMON_REMOTE_MODEL_FILES = [
    "config.json",
    "generation_config.json",
    "special_tokens_map.json",
    "tokenizer.json",
    "tokenizer.model",
    "tokenizer_config.json",
    "preprocessor_config.json",
    "processor_config.json",
    "chat_template.jinja",
    "merges.txt",
    "vocab.json",
]


@dataclass
class MooncakeStoreLoaderConfig:
    """Store settings used by ``MooncakeStoreConnector``.

    Extends :class:`MooncakeConfig` with optional standalone-client fields used
    by SGLang's remote weight loader (``MOONCAKE_STANDALONE_STORAGE`` /
    ``MOONCAKE_CLIENT``).
    """

    local_hostname: str
    metadata_server: str
    global_segment_size: int
    local_buffer_size: int
    protocol: str
    device_name: str
    master_server_address: str
    standalone_storage: bool = False
    client_server_address: str = ""
    preferred_segments: Optional[list[str]] = None
    replica_num: int = 1

    @classmethod
    def from_mooncake_config(
        cls,
        config: MooncakeConfig,
        *,
        standalone_storage: bool = False,
        client_server_address: str = "",
        preferred_segments: Optional[list[str]] = None,
        replica_num: int = 1,
        local_buffer_size: Optional[int] = None,
    ) -> "MooncakeStoreLoaderConfig":
        return cls(
            local_hostname=config.local_hostname,
            metadata_server=config.metadata_server,
            global_segment_size=config.global_segment_size,
            local_buffer_size=(
                local_buffer_size
                if local_buffer_size is not None
                else DEFAULT_CONNECTOR_LOCAL_BUFFER_SIZE
            ),
            protocol=config.protocol,
            device_name=config.device_name or "",
            master_server_address=config.master_server_address,
            standalone_storage=standalone_storage,
            client_server_address=client_server_address,
            preferred_segments=preferred_segments,
            replica_num=replica_num,
        )

    @classmethod
    def load_from_env(cls) -> "MooncakeStoreLoaderConfig":
        """Load from ``MOONCAKE_*`` env vars / ``MOONCAKE_CONFIG_PATH``."""
        base = MooncakeConfig.load_from_env()
        preferred = os.getenv("MOONCAKE_CONNECTOR_PREFERRED_SEGMENTS")
        preferred_segments = (
            [p for p in preferred.split(",") if p] if preferred else None
        )
        local_buffer = os.getenv("MOONCAKE_CONNECTOR_LOCAL_BUFFER_SIZE")
        return cls.from_mooncake_config(
            base,
            standalone_storage=_parse_bool(
                os.getenv("MOONCAKE_STANDALONE_STORAGE", "false")
            ),
            client_server_address=os.getenv("MOONCAKE_CLIENT", ""),
            preferred_segments=preferred_segments,
            replica_num=int(os.getenv("MOONCAKE_REPLICA_NUM", "1")),
            local_buffer_size=(
                _parse_segment_size(local_buffer)
                if local_buffer is not None
                else DEFAULT_CONNECTOR_LOCAL_BUFFER_SIZE
            ),
        )

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "MooncakeStoreLoaderConfig":
        """Build from a dict (e.g. ``model_loader_extra_config``)."""
        preferred = data.get("preferred_segments")
        if isinstance(preferred, str):
            preferred = [p for p in preferred.split(",") if p]

        if data.get("master_server_address") or data.get("master"):
            master = str(data.get("master_server_address") or data.get("master"))
            return cls(
                local_hostname=str(data.get("local_hostname", "localhost")),
                metadata_server=str(
                    data.get("metadata_server")
                    or data.get("te_meta_data_server")
                    or "P2PHANDSHAKE"
                ),
                global_segment_size=_parse_segment_size(
                    data.get("global_segment_size", DEFAULT_GLOBAL_SEGMENT_SIZE)
                ),
                local_buffer_size=_parse_segment_size(
                    data.get(
                        "local_buffer_size", DEFAULT_CONNECTOR_LOCAL_BUFFER_SIZE
                    )
                ),
                protocol=str(data.get("protocol", "tcp")),
                device_name=str(data.get("device_name", "")),
                master_server_address=master,
                standalone_storage=_parse_bool(
                    data.get("standalone_storage", False)
                ),
                client_server_address=str(data.get("client_server_address", "")),
                preferred_segments=preferred,
                replica_num=int(data.get("replica_num", 1)),
            )

        # Fall back to env when only partial extras are provided.
        cfg = cls.load_from_env()
        if "standalone_storage" in data:
            cfg.standalone_storage = _parse_bool(data["standalone_storage"])
        if "client_server_address" in data:
            cfg.client_server_address = str(data["client_server_address"])
        if preferred is not None:
            cfg.preferred_segments = list(preferred)
        if "replica_num" in data:
            cfg.replica_num = int(data["replica_num"])
        if "local_buffer_size" in data:
            cfg.local_buffer_size = _parse_segment_size(data["local_buffer_size"])
        return cfg


def parse_model_name(url: str) -> str:
    """Extract model name from a ``mooncake:///<model_name>`` URL."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme and parsed.scheme.lower() != "mooncake":
        raise ValueError(
            f"Unsupported URL scheme {parsed.scheme!r}; expected 'mooncake'"
        )
    name = parsed.path.lstrip("/")
    if not name and parsed.netloc:
        # mooncake://model-name (no path) → use netloc
        name = parsed.netloc
    if not name:
        raise ValueError(f"Missing model name in URL: {url!r}")
    return name


def rank_weight_key(model_name: str, rank: int, param_name: str) -> str:
    return f"{model_name}/keys/rank_{rank}/{param_name}"


def relative_rank_weight_key(rank: int, param_name: str) -> str:
    """Key relative to model_name (connector prefixes ``model_name/``)."""
    return f"keys/rank_{rank}/{param_name}"


def file_key(model_name: str, file_name: str) -> str:
    return f"{model_name}/files/{file_name}"
